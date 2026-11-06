if Code.ensure_loaded?(Mint.HTTP) do
  defmodule GRPC.Client.Adapters.Mint.ConnectionProcess do
    @moduledoc false

    #  This module is responsible for managing a connection with a gRPC server.
    #  It's also responsible for managing requests, which also includes checks for the
    #  connection/request window size, splitting a given payload into appropriate sized chunks
    #  and streaming those to the server using an internal queue.

    use GenServer

    alias GRPC.Client.Adapter
    alias GRPC.Client.Adapters.Mint.ConnectionProcess.State
    alias GRPC.Client.Adapters.Mint.StreamResponseProcess
    alias GRPC.Client.Telemetry

    require Logger
    require State

    @connection_closed_error GRPC.RPCError.exception(
                               GRPC.Status.unavailable(),
                               "the connection is closed"
                             )
    @request_cancelled_error GRPC.RPCError.exception(
                               GRPC.Status.cancelled(),
                               "the request was cancelled"
                             )
    @goaway_error GRPC.RPCError.exception(
                    GRPC.Status.unavailable(),
                    "the server is closing the connection, so the request body cannot be sent"
                  )
    # A GOAWAY tells us which requests the server never started; those are
    # safe to retry, so they fail as UNAVAILABLE rather than with Mint's error.
    @unprocessed_error GRPC.RPCError.exception(
                         GRPC.Status.unavailable(),
                         "the request was not processed before the server closed the connection"
                       )
    @stream_response_dead_event [:grpc, :client, :mint, :stream_response, :dead]
    @goaway_event [:grpc, :client, :mint, :goaway]
    @reconnect_stop_event [:grpc, :client, :mint, :reconnect, :stop]
    @reconnect_error_event [:grpc, :client, :mint, :reconnect, :error]
    @reconnect_exhausted_event [:grpc, :client, :mint, :reconnect, :exhausted]
    @mint_default_connect_timeout 30_000

    @doc """
    Starts and link connection process
    """
    def start_link(scheme, host, port, opts \\ []) do
      opts = Keyword.put(opts, :parent, self())
      GenServer.start_link(__MODULE__, {scheme, host, port, opts})
    end

    @doc """
    Sends a request to the connected server.

    If the call exits (the connection process is busy past the call timeout, or
    dead), the exit reason has the headers and body replaced with `:redacted`.

    ## Options

      * :stream_response_pid (required) - the process to where send the responses coming from the connection will be sent to be processed
    """
    def request(pid, method, path, headers, body, opts \\ []) do
      Adapter.call(pid, {:request, method, path, headers, body, opts})
    end

    @doc """
    Closes the given connection.

    Returns `:ok` before the socket is closed. All exits are caught, so a
    connection process that is dead or does not reply in 5 seconds also gives
    `:ok`. A busy process still stops when it reads the message.
    """
    def disconnect(pid) do
      GenServer.call(pid, :disconnect)
    catch
      :exit, _reason -> :ok
    end

    @doc """
    Streams a chunk of the request body on the connection or signals the end of the body.
    """
    def stream_request_body(pid, request_ref, body) do
      Adapter.call(pid, {:stream_body, request_ref, body})
    end

    @doc """
    cancels an open request request

    `timeout` bounds the wait for the connection process to acknowledge the
    cancellation, for callers that are already past a deadline of their own.
    """
    def cancel(pid, request_ref, timeout \\ 5_000) do
      GenServer.call(pid, {:cancel_request, request_ref}, timeout)
    end

    ## Callbacks

    @impl true
    def init({scheme, host, port, opts}) do
      {retry, opts} = Keyword.pop(opts, :retry, 0)
      {notify_drain, opts} = Keyword.pop(opts, :notify_drain, false)
      {metadata, opts} = Keyword.pop(opts, :grpc_transport_metadata, %{})
      metadata = Map.put(metadata, :transport_ref, self())
      Telemetry.execute(:started, %{generation: 0, active_streams: 0}, metadata)
      {keepalive_opts, opts} = Keyword.split(opts, [:keepalive, :keepalive_tolerance])

      case connect(scheme, host, port, opts) do
        {:ok, conn} ->
          state_opts = [
            parent: opts[:parent],
            scheme: scheme,
            host: host,
            port: port,
            connect_opts: opts,
            retry: retry,
            notify_drain: notify_drain,
            telemetry_metadata: metadata
          ]

          Telemetry.execute(:connected, %{generation: 1}, Map.put(metadata, :reconnect, false))

          {:ok,
           conn
           |> State.new(state_opts ++ keepalive_opts)
           |> observe_settings()
           |> schedule_keepalive()}

        {:error, reason} ->
          Logger.error(
            "unable to establish a connection to #{scheme}://#{host}:#{port}. reason: #{inspect(reason)}"
          )

          connect_failed(metadata, reason)
          {:stop, reason}
      end
    catch
      :exit, reason ->
        Logger.error(
          "unable to establish a connection to #{scheme}://#{host}:#{port}. reason: #{inspect(reason)}"
        )

        metadata = Map.put(opts[:grpc_transport_metadata] || %{}, :transport_ref, self())
        connect_failed(metadata, reason)
        {:stop, reason}
    end

    @impl true
    def handle_call(:disconnect, from, state) do
      # TODO add a code to if disconnect is brutal we just  stop if is friendly we wait for pending requests
      # :ssl.close/1 can wait up to 5 seconds for the peer, so reply before the close.
      GenServer.reply(from, :ok)
      {:ok, conn} = Mint.HTTP.close(state.conn)
      Enum.each(state.retired, fn {_id, retired_conn} -> Mint.HTTP.close(retired_conn) end)
      {:stop, :normal, %{State.update_conn(state, conn) | retired: %{}}}
    end

    def handle_call(_request, _from, %{conn: %Mint.HTTP2{state: :closed}} = state) do
      {:reply, {:error, @connection_closed_error}, state}
    end

    def handle_call(
          {:request, method, path, headers, :stream, opts},
          _from,
          state
        ) do
      case Mint.HTTP.request(state.conn, method, path, headers, :stream) do
        {:ok, conn, request_ref} ->
          stream_response_pid = opts[:stream_response_pid]
          monitor_ref = if is_pid(stream_response_pid), do: Process.monitor(stream_response_pid)

          new_state =
            state
            |> State.update_conn(conn)
            |> State.put_empty_ref_state(request_ref, stream_response_pid, monitor_ref)

          emit_streams(new_state)
          {:reply, {:ok, %{request_ref: request_ref}}, new_state}

        {:error, conn, reason} ->
          new_state = State.update_conn(state, conn)
          {:reply, {:error, reason}, new_state}
      end
    end

    def handle_call(
          {:request, method, path, headers, body, opts},
          _from,
          state
        ) do
      case Mint.HTTP.request(state.conn, method, path, headers, :stream) do
        {:ok, conn, request_ref} ->
          queue = :queue.in({request_ref, body, nil}, state.request_stream_queue)
          stream_response_pid = opts[:stream_response_pid]
          monitor_ref = if is_pid(stream_response_pid), do: Process.monitor(stream_response_pid)

          new_state =
            state
            |> State.update_conn(conn)
            |> State.update_request_stream_queue(queue)
            |> State.put_empty_ref_state(request_ref, stream_response_pid, monitor_ref)

          emit_streams(new_state)

          {:reply, {:ok, %{request_ref: request_ref}}, new_state,
           {:continue, :process_request_stream_queue}}

        {:error, conn, reason} ->
          new_state = State.update_conn(state, conn)
          {:reply, {:error, reason}, new_state}
      end
    end

    def handle_call({:stream_body, request_ref, :eof}, _from, state) do
      if State.has_request_ref(state, request_ref) do
        conn_id = State.conn_id(state, request_ref)

        case Mint.HTTP.stream_request_body(conn_for(state, conn_id), request_ref, :eof) do
          {:ok, conn} ->
            {:reply, :ok, put_conn(state, conn_id, conn)}

          {:error, conn, %Mint.HTTPError{reason: :unknown_request_to_stream}} ->
            {:reply, :ok, put_conn(state, conn_id, conn)}

          {:error, conn, %Mint.HTTPError{reason: :closed_for_writing}} ->
            state = state |> put_conn(conn_id, conn) |> drop_unsendable_stream(request_ref)
            {:reply, {:error, @goaway_error}, state, {:continue, :check_connection_status}}

          {:error, conn, error} ->
            {:reply, {:error, error}, put_conn(state, conn_id, conn)}
        end
      else
        {:reply, :ok, state}
      end
    end

    def handle_call({:stream_body, request_ref, body}, from, state) do
      queue = :queue.in({request_ref, body, from}, state.request_stream_queue)

      {:noreply, State.update_request_stream_queue(state, queue),
       {:continue, :process_request_stream_queue}}
    end

    def handle_call({:cancel_request, request_ref}, _from, state)
        when State.has_request_ref(state, request_ref) do
      conn_id = State.conn_id(state, request_ref)
      state = process_response({:done, request_ref}, state)

      case Mint.HTTP2.cancel_request(conn_for(state, conn_id), request_ref) do
        {:ok, conn} ->
          {:reply, :ok, state |> put_conn(conn_id, conn) |> close_if_drained(conn_id)}

        {:error, conn, error} ->
          {:reply, {:error, error}, state |> put_conn(conn_id, conn) |> close_if_drained(conn_id)}
      end
    end

    def handle_call({:cancel_request, _request_ref}, _from, state) do
      {:reply, :ok, state}
    end

    @impl true
    def handle_info(:reconnect, state) do
      maybe_attempt_reconnect(state)
    end

    def handle_info(:keepalive, state) do
      state
      |> schedule_keepalive()
      |> handle_keepalive()
    end

    def handle_info({:DOWN, monitor_ref, :process, pid, reason}, state) do
      case State.find_request_ref_by_monitor_ref(state, monitor_ref) do
        nil ->
          {:noreply, state}

        request_ref ->
          new_state = cancel_dead_stream(state, request_ref, pid, reason)
          {:noreply, new_state}
      end
    end

    def handle_info(message, state) do
      case Mint.HTTP.stream(state.conn, message) do
        :unknown ->
          stream_retired(message, state)

        {:ok, conn, responses} ->
          state
          |> State.update_conn(conn)
          |> acknowledge_pongs(responses)
          |> process_responses(responses)
          |> observe_settings()
          |> check_connection_status(nil)

        {:error, conn, error, responses} ->
          state
          |> State.update_conn(conn)
          |> process_responses(responses)
          |> check_connection_status(error)
      end
    end

    # A message that is not for the active connection may be for one the
    # server retired with a GOAWAY that still answers requests in flight.
    defp stream_retired(message, state) do
      found =
        Enum.find_value(state.retired, fn {id, conn} ->
          case Mint.HTTP.stream(conn, message) do
            :unknown -> nil
            {:ok, conn, responses} -> {id, conn, responses}
            {:error, conn, _error, responses} -> {id, conn, responses}
          end
        end)

      case found do
        nil ->
          Logger.debug(fn -> "Received unknown message: " <> inspect(message) end)
          {:noreply, state}

        {id, conn, responses} ->
          state
          |> put_conn(id, conn)
          |> process_responses(responses)
          |> close_if_drained(id)
          |> check_request_stream_queue()
      end
    end

    defp process_responses(state, responses),
      do: Enum.reduce(responses, state, &process_response/2)

    @impl true
    def handle_continue(:check_connection_status, state) do
      check_connection_status(state, nil)
    end

    def handle_continue(:process_request_stream_queue, state) do
      {{:value, request}, queue} = :queue.out(state.request_stream_queue)
      {ref, body, _from} = request
      window_size = get_window_size(conn_for(state, State.conn_id(state, ref)), ref)
      dequeued_state = State.update_request_stream_queue(state, queue)

      cond do
        # Do nothing, wait for server (on stream/2) to give us more window size
        window_size == 0 ->
          {:noreply, state}

        IO.iodata_length(body) > window_size ->
          chunk_body_and_enqueue_rest(request, dequeued_state)

        true ->
          stream_body_and_reply(request, dequeued_state)
      end
    end

    @impl true
    def terminate(reason, state) do
      Telemetry.execute(
        :stopped,
        %{streams_terminated: map_size(state.requests)},
        Map.put(state.telemetry_metadata, :reason, reason)
      )

      :normal
    end

    defp schedule_keepalive(%{keepalive: interval} = state) when is_integer(interval) do
      Process.send_after(self(), :keepalive, interval)
      state
    end

    defp schedule_keepalive(state), do: state

    defp handle_keepalive(%{conn: %Mint.HTTP2{state: :closed}} = state), do: {:noreply, state}

    defp handle_keepalive(%{pings_unacked: unacked, keepalive_tolerance: tolerance} = state)
         when is_integer(tolerance) and unacked >= tolerance do
      Logger.warning(
        "Closing connection to #{state.scheme}://#{state.host}:#{state.port} after #{unacked} unacknowledged keepalive pings"
      )

      close_connection(state, :keepalive_timeout)
    end

    defp handle_keepalive(state) do
      case Mint.HTTP2.ping(state.conn) do
        {:ok, conn, _ref} ->
          {:noreply, %{State.update_conn(state, conn) | pings_unacked: state.pings_unacked + 1}}

        {:error, conn, reason} ->
          Logger.warning(
            "Closing connection to #{state.scheme}://#{state.host}:#{state.port} after failing to send keepalive ping: #{inspect(reason)}"
          )

          state
          |> State.update_conn(conn)
          |> close_connection(reason)
      end
    end

    defp close_connection(state, reason) do
      {:ok, conn} = Mint.HTTP.close(state.conn)

      state
      |> State.update_conn(conn)
      |> finish_all_pending_requests(reason)
    end

    defp acknowledge_pongs(state, responses) do
      pongs = Enum.count(responses, &match?({:pong, _ref}, &1))
      %{state | pings_unacked: state.pings_unacked - pongs}
    end

    # Frames may still arrive for a stream whose state was already dropped
    # (e.g. cancelled after its response process died mid-batch). Ignore them
    # instead of crashing the whole connection.
    defp process_response(response, state)
         when not State.has_request_ref(state, elem(response, 1)) do
      state
    end

    defp process_response({:status, request_ref, 200}, state) do
      State.update_response_status(state, request_ref, 200)
    end

    defp process_response({:status, request_ref, status}, state) do
      state
      |> State.update_response_status(request_ref, status)
      |> consume_or_cancel_stream(
        request_ref,
        :error,
        GRPC.RPCError.exception(GRPC.Status.internal(), "status got is #{status} instead of 200")
      )
    end

    defp process_response({:headers, request_ref, headers}, state) do
      if State.empty_headers?(state, request_ref) do
        new_state = State.update_response_headers(state, request_ref, headers)
        consume_or_cancel_stream(new_state, request_ref, :headers, headers)
      else
        consume_or_cancel_stream(state, request_ref, :trailers, headers)
      end
    end

    defp process_response({:data, request_ref, new_data}, state) do
      consume_or_cancel_stream(state, request_ref, :data, new_data)
    end

    defp process_response({:error, request_ref, reason}, state) do
      state
      |> State.stream_response_pid(request_ref)
      |> end_stream_response(stream_error(reason))

      drop_request(state, request_ref)
    end

    defp process_response({:done, request_ref}, state) do
      state
      |> State.stream_response_pid(request_ref)
      |> StreamResponseProcess.done()

      drop_request(state, request_ref)
    end

    defp consume_or_cancel_stream(state, request_ref, type, data) do
      pid = State.stream_response_pid(state, request_ref)

      case StreamResponseProcess.consume(pid, type, data) do
        :ok -> state
        :cancel -> cancel_refused_stream(state, request_ref)
        {:error, reason} -> cancel_dead_stream(state, request_ref, pid, reason)
      end
    end

    # The response process refused the response (a message over
    # `:max_message_length`) and has queued the error. Reset the request so the
    # server stops sending, and ignore what is already in flight.
    defp cancel_refused_stream(state, request_ref) do
      drop_and_cancel(state, request_ref)
    end

    # Drop the request and reset its stream on the connection it runs on.
    defp drop_and_cancel(state, request_ref) do
      conn_id = State.conn_id(state, request_ref)
      state = drop_request(state, request_ref)

      conn =
        case Mint.HTTP2.cancel_request(conn_for(state, conn_id), request_ref) do
          {:ok, conn} -> conn
          {:error, conn, _error} -> conn
        end

      state |> put_conn(conn_id, conn) |> close_if_drained(conn_id)
    end

    defp cancel_dead_stream(state, request_ref, pid, reason) do
      Logger.warning(
        "stream response process #{inspect(pid)} is not alive (#{inspect(reason)}), cancelling request #{inspect(request_ref)}"
      )

      :telemetry.execute(@stream_response_dead_event, %{}, %{
        request_ref: request_ref,
        stream_response_pid: pid,
        reason: reason
      })

      drop_and_cancel(state, request_ref)
    end

    defp drop_request(state, request_ref) do
      {_ref, state} = State.pop_ref(state, request_ref)
      emit_streams(state)
      drop_queued_request_chunks(state, request_ref)
    end

    defp drop_queued_request_chunks(state, request_ref) do
      {dropped, kept} =
        state.request_stream_queue
        |> :queue.to_list()
        |> Enum.split_with(&match?({^request_ref, _body, _from}, &1))

      for {_ref, _body, from} <- dropped, not is_nil(from) do
        GenServer.reply(from, {:error, @request_cancelled_error})
      end

      State.update_request_stream_queue(state, :queue.from_list(kept))
    end

    defp chunk_body_and_enqueue_rest({request_ref, body, from}, state) do
      conn_id = State.conn_id(state, request_ref)
      conn = conn_for(state, conn_id)
      {head, tail} = chunk_body(body, get_window_size(conn, request_ref))

      case Mint.HTTP.stream_request_body(conn, request_ref, head) do
        {:ok, conn} ->
          queue = :queue.in_r({request_ref, tail, from}, state.request_stream_queue)

          new_state =
            state
            |> put_conn(conn_id, conn)
            |> State.update_request_stream_queue(queue)

          {:noreply, new_state}

        {:error, conn, %Mint.HTTPError{reason: :closed_for_writing}} ->
          state |> put_conn(conn_id, conn) |> fail_unsendable_stream(request_ref, from)

        {:error, conn, error} ->
          if from != nil do
            # We need an explicit reply here because the process that called this GenServer
            # isn't the same one that's expecting the reply.
            GenServer.reply(from, {:error, error})
          else
            state
            |> State.stream_response_pid(request_ref)
            |> StreamResponseProcess.consume(:error, error)
          end

          {:noreply, put_conn(state, conn_id, conn)}
      end
    end

    defp stream_body_and_reply({request_ref, body, from}, state) do
      send_eof? = is_nil(from)
      conn_id = State.conn_id(state, request_ref)

      case stream_body(conn_for(state, conn_id), request_ref, body, send_eof?) do
        {:ok, conn} ->
          if not send_eof? do
            GenServer.reply(from, :ok)
          end

          check_request_stream_queue(put_conn(state, conn_id, conn))

        {:error, conn, %Mint.HTTPError{reason: :closed_for_writing}} ->
          state |> put_conn(conn_id, conn) |> fail_unsendable_stream(request_ref, from)

        {:error, conn, error} ->
          if not send_eof? do
            GenServer.reply(from, {:error, error})
          else
            state
            |> State.stream_response_pid(request_ref)
            |> StreamResponseProcess.consume(:error, error)
          end

          check_request_stream_queue(put_conn(state, conn_id, conn))
      end
    end

    # After GOAWAY, Mint takes no more request body for any stream, not even `:eof`,
    # and it sends no RST_STREAM. The server waits for the body, so the call and the
    # drain would not end. Drop the stream and fail it. The server sees the connection
    # close when the drain ends.
    defp fail_unsendable_stream(state, request_ref, from) do
      state = drop_unsendable_stream(state, request_ref)
      if from, do: GenServer.reply(from, {:error, @goaway_error})
      check_connection_status(state, nil)
    end

    defp drop_unsendable_stream(state, request_ref) do
      if State.has_request_ref(state, request_ref) do
        state
        |> State.stream_response_pid(request_ref)
        |> end_stream_response(@goaway_error)
      end

      drop_and_cancel(state, request_ref)
    end

    defp stream_body(conn, request_ref, body, true = _stream_eof?) do
      case Mint.HTTP.stream_request_body(conn, request_ref, body) do
        {:ok, conn} -> Mint.HTTP.stream_request_body(conn, request_ref, :eof)
        error -> error
      end
    end

    defp stream_body(conn, request_ref, body, false = _stream_eof?) do
      Mint.HTTP.stream_request_body(conn, request_ref, body)
    end

    def check_request_stream_queue(state) do
      if :queue.is_empty(state.request_stream_queue) do
        {:noreply, state}
      else
        {:noreply, state, {:continue, :process_request_stream_queue}}
      end
    end

    defp chunk_body(body, bytes_length) do
      case body do
        <<head::binary-size(^bytes_length), tail::binary>> -> {head, tail}
        _other -> {body, <<>>}
      end
    end

    def get_window_size(conn, ref) do
      min(
        Mint.HTTP2.get_window_size(conn, {:request, ref}),
        Mint.HTTP2.get_window_size(conn, :connection)
      )
    end

    # Fails the requests on the active connection. Requests still running on a
    # connection retired by a GOAWAY are not affected: that is a different one.
    defp finish_all_pending_requests(state, reason) do
      active = State.refs_on(state, :active)
      count = length(active)
      metadata = Map.put(state.telemetry_metadata, :reason, reason)
      Telemetry.execute(:down, %{streams_terminated: count}, metadata)

      {queued, kept} =
        state.request_stream_queue
        |> :queue.to_list()
        |> Enum.split_with(fn {ref, _body, _from} -> State.conn_id(state, ref) == :active end)

      for {_ref, _body, from} <- queued, not is_nil(from) do
        GenServer.reply(from, {:error, @connection_closed_error})
      end

      clean_state =
        state
        |> State.update_request_stream_queue(:queue.from_list(kept))
        |> fail_requests(active, @connection_closed_error)
        |> Map.put(:draining_in_place, false)

      if count > 0, do: emit_streams(clean_state)

      # A failed attempt already scheduled the next retry; ignore duplicate close events.
      if clean_state.retry_attempt > 0 do
        {:noreply, clean_state}
      else
        maybe_attempt_reconnect(clean_state)
      end
    end

    defp fail_requests(state, refs, error) do
      Enum.reduce(refs, state, fn ref, acc ->
        acc
        |> State.stream_response_pid(ref)
        |> end_stream_response(error)

        {_request, acc} = State.pop_ref(acc, ref)
        acc
      end)
    end

    # The process is about to stop: nothing will deliver the answers of the
    # requests still running on retired connections, so fail them instead of
    # leaving callers waiting.
    defp fail_retired(state) do
      state.retired
      |> Enum.reduce(state, fn {id, conn}, acc ->
        Mint.HTTP.close(conn)
        fail_requests(acc, State.refs_on(acc, id), @connection_closed_error)
      end)
      |> Map.put(:retired, %{})
    end

    defp end_stream_response(pid, error) do
      StreamResponseProcess.consume(pid, :error, error)
      StreamResponseProcess.done(pid)
    end

    # The pool already opened a replacement for a draining connection.
    defp maybe_attempt_reconnect(%{draining: true} = state) do
      send(state.parent, {:elixir_grpc, :connection_down, self()})
      {:stop, :normal, fail_retired(state)}
    end

    defp maybe_attempt_reconnect(%{retry: 0} = state) do
      send(state.parent, {:elixir_grpc, :connection_down, self()})
      {:stop, :normal, fail_retired(state)}
    end

    defp maybe_attempt_reconnect(%{retry_attempt: attempt} = state)
         when State.retries_exhausted?(state) do
      Logger.warning(
        "Connection retry exhausted (#{attempt}/#{state.retry}) for #{state.scheme}://#{state.host}:#{state.port}"
      )

      :telemetry.execute(
        @reconnect_exhausted_event,
        %{},
        reconnect_metadata(state, attempt)
      )

      send(state.parent, {:elixir_grpc, :connection_down, self()})
      {:stop, :normal, fail_retired(state)}
    end

    defp maybe_attempt_reconnect(state) do
      next_attempt = state.retry_attempt + 1

      Logger.info(
        "Attempting reconnection #{next_attempt}/#{state.retry} to #{state.scheme}://#{state.host}:#{state.port}"
      )

      start = System.monotonic_time()

      case connect(state.scheme, state.host, state.port, state.connect_opts) do
        {:ok, conn} ->
          Logger.info("Reconnected successfully to #{state.scheme}://#{state.host}:#{state.port}")

          :telemetry.execute(
            @reconnect_stop_event,
            %{duration: System.monotonic_time() - start},
            reconnect_metadata(state, next_attempt)
          )

          generation = state.generation + 1

          Telemetry.execute(
            :connected,
            %{generation: generation},
            Map.put(state.telemetry_metadata, :reconnect, true)
          )

          new_state = %{
            state
            | conn: conn,
              retry_attempt: 0,
              generation: generation,
              max_concurrent_streams: :unknown,
              settings_known?: false,
              pings_unacked: 0
          }

          {:noreply, observe_settings(new_state)}

        {:error, reason} ->
          Logger.warning(
            "Reconnection attempt #{next_attempt}/#{state.retry} failed: #{inspect(reason)}"
          )

          :telemetry.execute(
            @reconnect_error_event,
            %{duration: System.monotonic_time() - start},
            Map.put(reconnect_metadata(state, next_attempt), :reason, reason)
          )

          maybe_connect_error(state.telemetry_metadata, reason)
          timeout = state.retry_timeout_ms || retry_timeout(next_attempt)
          Process.send_after(self(), :reconnect, timeout)
          {:noreply, %{state | retry_attempt: next_attempt}}
      end
    end

    defp reconnect_metadata(state, attempt) do
      %{
        scheme: state.scheme,
        host: state.host,
        port: state.port,
        attempt: attempt,
        max: state.retry
      }
    end

    @doc false
    def retry_timeout(attempt) do
      timeout =
        if attempt < 11 do
          :math.pow(1.6, attempt - 1) * 1000
        else
          120_000
        end

      jitter = (:rand.uniform_real() - 0.5) / 2.5
      round(timeout + jitter * timeout)
    end

    defp check_connection_status(state, reason) do
      cond do
        Mint.HTTP.open?(state.conn) ->
          check_request_stream_queue(state)

        # Readable but not writable: the server sent a GOAWAY. It still answers the
        # streams it accepted, it just takes no new ones on this connection.
        is_nil(reason) and Mint.HTTP.open?(state.conn, :read) ->
          handle_goaway(state)

        true ->
          finish_all_pending_requests(state, reason || @connection_closed_error)
      end
    end

    # Opening a replacement for this GOAWAY already failed: `conn` keeps draining
    # and the last request to finish takes it to the regular reconnect.
    defp handle_goaway(%{draining_in_place: true} = state), do: drain_in_place(state)

    # Open a new connection for new requests right away and let the retired one
    # finish the requests in flight on it, so no call lands on a connection that
    # takes no new streams. This is a planned handover by the server (e.g. grpc-go's
    # MaxConnectionAge), not a failure, so it happens regardless of the :retry
    # setting and without its backoff.
    defp handle_goaway(state) do
      in_flight = length(State.refs_on(state, :active))

      case connect(state.scheme, state.host, state.port, state.connect_opts) do
        {:ok, new_conn} ->
          emit_goaway(state, in_flight, true)
          state |> retire_conn(new_conn) |> check_request_stream_queue()

        {:error, reason} ->
          Logger.warning(
            "Server at #{state.scheme}://#{state.host}:#{state.port} is closing the connection " <>
              "and opening a new one failed: #{inspect(reason)}; finishing the requests in flight first"
          )

          emit_goaway(state, in_flight, false)
          maybe_connect_error(state.telemetry_metadata, reason)
          drain_in_place(%{state | draining_in_place: true})
      end
    end

    defp retire_conn(state, new_conn) do
      id = make_ref()
      active = State.refs_on(state, :active)

      # Mint takes no more request body after a GOAWAY, so requests still waiting
      # to send theirs can never finish. Fail them now instead of letting them
      # block the queue for the new connection.
      state =
        state
        |> queued_refs(active)
        |> Enum.reduce(state, &drop_unsendable_stream(&2, &1))

      generation = state.generation + 1

      Telemetry.execute(
        :connected,
        %{generation: generation},
        Map.put(state.telemetry_metadata, :reconnect, true)
      )

      state
      |> State.move_requests(State.refs_on(state, :active), id)
      |> Map.update!(:retired, &Map.put(&1, id, state.conn))
      |> Map.merge(%{
        conn: new_conn,
        generation: generation,
        max_concurrent_streams: :unknown,
        settings_known?: false,
        pings_unacked: 0
      })
      |> observe_settings()
      |> close_if_drained(id)
    end

    # Without a replacement, behave like a plain drain: new calls fail (or, with
    # the pool, go to another connection) until the server closes this one.
    defp drain_in_place(state) do
      if State.refs_on(state, :active) == [] do
        finish_all_pending_requests(state, @connection_closed_error)
      else
        state |> notify_draining() |> check_request_stream_queue()
      end
    end

    defp queued_refs(state, refs) do
      queued =
        for {ref, _body, _from} <- :queue.to_list(state.request_stream_queue),
            into: MapSet.new(),
            do: ref

      Enum.filter(refs, &MapSet.member?(queued, &1))
    end

    # Once a retired connection has no requests left, or the server closed it,
    # close it and fail whatever was still waiting on it.
    defp close_if_drained(state, :active), do: state

    defp close_if_drained(state, id) do
      case Map.fetch(state.retired, id) do
        :error ->
          state

        {:ok, conn} ->
          refs = State.refs_on(state, id)

          if refs == [] or not Mint.HTTP.open?(conn, :read) do
            Mint.HTTP.close(conn)

            state
            |> fail_requests(refs, @connection_closed_error)
            |> Map.update!(:retired, &Map.delete(&1, id))
          else
            state
          end
      end
    end

    defp emit_goaway(state, in_flight, reconnected) do
      error_code =
        case state.conn.state do
          {:goaway, error_code, _debug_data} -> error_code
          _other -> nil
        end

      :telemetry.execute(@goaway_event, %{in_flight: in_flight}, %{
        scheme: state.scheme,
        host: state.host,
        port: state.port,
        error_code: error_code,
        reconnected: reconnected
      })
    end

    defp conn_for(state, :active), do: state.conn
    defp conn_for(state, id), do: Map.fetch!(state.retired, id)

    defp put_conn(state, :active, conn), do: State.update_conn(state, conn)
    # A retired connection that was already closed stays closed.
    defp put_conn(%{retired: retired} = state, id, conn) when is_map_key(retired, id),
      do: put_in(state.retired[id], conn)

    defp put_conn(state, _id, _conn), do: state

    defp stream_error(%Mint.HTTPError{reason: :unprocessed}), do: @unprocessed_error
    defp stream_error(reason), do: reason

    # Tells the pool, once, to send new calls to another connection.
    defp notify_draining(%{notify_drain: true, draining: false} = state) do
      send(state.parent, {:elixir_grpc, :draining, self()})
      %{state | draining: true}
    end

    defp notify_draining(state), do: state

    defp observe_settings(state) do
      max =
        normalize_max_streams(Mint.HTTP2.get_server_setting(state.conn, :max_concurrent_streams))

      if not state.settings_known? or max != state.max_concurrent_streams do
        Telemetry.execute(
          :settings,
          %{},
          Map.put(state.telemetry_metadata, :max_concurrent_streams, max)
        )
      end

      %{state | settings_known?: true, max_concurrent_streams: max}
    end

    defp emit_streams(state) do
      Telemetry.execute(:streams, %{active: map_size(state.requests)}, state.telemetry_metadata)
    end

    # Mint 1.x reports 100 both before an explicit peer value and when the peer
    # explicitly advertises 100. Treat it as unknown rather than inventing
    # finite capacity; an upstream SETTINGS-presence hook would remove this gap.
    defp normalize_max_streams(100), do: :unknown
    defp normalize_max_streams(:infinity), do: :infinity
    defp normalize_max_streams(value) when is_integer(value) and value > 0, do: value
    defp normalize_max_streams(_value), do: :unknown

    defp connect(scheme, host, port, opts) do
      timeout = get_in(opts, [:transport_opts, :timeout]) || @mint_default_connect_timeout
      deadline = connect_deadline(timeout)

      with {:ok, conn} <-
             Mint.HTTP.connect(
               scheme,
               host,
               port,
               Keyword.merge(opts, mode: :passive, protocols: [:http2])
             ),
           {:ok, conn} <- await_server_preface(conn, deadline) do
        Mint.HTTP.set_mode(conn, :active)
      end
    end

    defp await_server_preface(%Mint.HTTP2{state: :handshaking} = conn, deadline) do
      case Mint.HTTP.recv(conn, 0, remaining_time(deadline)) do
        {:ok, conn, []} ->
          await_server_preface(conn, deadline)

        {:error, _conn, reason, _responses} ->
          {:ok, _conn} = Mint.HTTP.close(conn)
          {:error, reason}
      end
    end

    defp await_server_preface(conn, _deadline) do
      if Mint.HTTP.open?(conn),
        do: {:ok, conn},
        else: {:error, %Mint.TransportError{reason: :closed}}
    end

    defp connect_deadline(:infinity), do: :infinity
    defp connect_deadline(timeout), do: System.monotonic_time(:millisecond) + timeout

    defp remaining_time(:infinity), do: :infinity
    defp remaining_time(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

    defp connect_failed(metadata, reason) do
      maybe_connect_error(metadata, reason)
      Telemetry.execute(:stopped, %{streams_terminated: 0}, Map.put(metadata, :reason, reason))
    end

    defp maybe_connect_error(metadata, reason) do
      if stage = connect_stage(reason) do
        Telemetry.execute(
          :connect_error,
          %{},
          Map.merge(metadata, %{stage: stage, reason: reason})
        )
      end
    end

    defp connect_stage(%Mint.TransportError{reason: reason}), do: connect_stage(reason)
    defp connect_stage(%Mint.HTTPError{}), do: :http2
    defp connect_stage(reason) when reason in [:nxdomain, :host_not_found], do: :resolve

    defp connect_stage(reason) when reason in [:econnrefused, :enetunreach, :ehostunreach],
      do: :tcp

    defp connect_stage({:tls_alert, _detail}), do: :tls
    defp connect_stage({_tag, reason}), do: connect_stage(reason)
    defp connect_stage(_reason), do: nil
  end
end
