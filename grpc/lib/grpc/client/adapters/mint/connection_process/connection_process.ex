if Code.ensure_loaded?(Mint.HTTP) do
  defmodule GRPC.Client.Adapters.Mint.ConnectionProcess do
    @moduledoc false

    #  This module is responsible for managing a connection with a gRPC server.
    #  It's also responsible for managing requests, which also includes checks for the
    #  connection/request window size, splitting a given payload into appropriate sized chunks
    #  and streaming those to the server using an internal queue.

    use GenServer

    alias GRPC.Client.Adapters.Mint.ConnectionProcess.State
    alias GRPC.Client.Adapters.Mint.StreamResponseProcess

    require Logger
    require State

    @connection_closed_error GRPC.RPCError.exception(
                               GRPC.Status.unavailable(),
                               "the connection is closed"
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

    @doc """
    Starts and link connection process
    """
    def start_link(scheme, host, port, opts \\ []) do
      opts = Keyword.put(opts, :parent, self())
      GenServer.start_link(__MODULE__, {scheme, host, port, opts})
    end

    @doc """
    Sends a request to the connected server.

    ## Options

      * :stream_response_pid (required) - the process to where send the responses coming from the connection will be sent to be processed
    """
    def request(pid, method, path, headers, body, opts \\ []) do
      GenServer.call(pid, {:request, method, path, headers, body, opts})
    end

    @doc """
    Closes the given connection.
    """
    def disconnect(pid) do
      GenServer.call(pid, :disconnect)
    end

    @doc """
    Streams a chunk of the request body on the connection or signals the end of the body.
    """
    def stream_request_body(pid, request_ref, body) do
      GenServer.call(pid, {:stream_body, request_ref, body})
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

      case Mint.HTTP.connect(scheme, host, port, opts) do
        {:ok, conn} ->
          state_opts = [
            parent: opts[:parent],
            scheme: scheme,
            host: host,
            port: port,
            connect_opts: opts,
            retry: retry
          ]

          {:ok, State.new(conn, state_opts)}

        {:error, reason} ->
          Logger.error(
            "unable to establish a connection to #{scheme}://#{host}:#{port}. reason: #{inspect(reason)}"
          )

          {:stop, reason}
      end
    catch
      :exit, reason ->
        Logger.error(
          "unable to establish a connection to #{scheme}://#{host}:#{port}. reason: #{inspect(reason)}"
        )

        {:stop, reason}
    end

    @impl true
    def handle_call(:disconnect, _from, state) do
      # TODO add a code to if disconnect is brutal we just  stop if is friendly we wait for pending requests
      {:ok, conn} = Mint.HTTP.close(state.conn)
      Enum.each(state.draining, fn {_id, draining_conn} -> Mint.HTTP.close(draining_conn) end)
      {:stop, :normal, :ok, %{State.update_conn(state, conn) | draining: %{}}}
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
          new_state =
            state
            |> State.update_conn(conn)
            |> State.put_empty_ref_state(request_ref, opts[:stream_response_pid])

          {:reply, {:ok, %{request_ref: request_ref}}, new_state}

        {:error, conn, reason} ->
          new_state = State.update_conn(state, conn)
          {:reply, {:error, request_error(reason)}, new_state}
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

          new_state =
            state
            |> State.update_conn(conn)
            |> State.update_request_stream_queue(queue)
            |> State.put_empty_ref_state(request_ref, opts[:stream_response_pid])

          {:reply, {:ok, %{request_ref: request_ref}}, new_state,
           {:continue, :process_request_stream_queue}}

        {:error, conn, reason} ->
          new_state = State.update_conn(state, conn)
          {:reply, {:error, request_error(reason)}, new_state}
      end
    end

    def handle_call({:stream_body, request_ref, :eof}, _from, state) do
      case Mint.HTTP.stream_request_body(state.conn, request_ref, :eof) do
        {:ok, conn} ->
          {:reply, :ok, State.update_conn(state, conn)}

        {:error, conn, error} ->
          {:reply, {:error, error}, State.update_conn(state, conn)}
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

    def handle_info(message, state) do
      case Mint.HTTP.stream(state.conn, message) do
        :unknown ->
          stream_draining(message, state)

        {:ok, conn, responses} ->
          state
          |> State.update_conn(conn)
          |> process_responses(responses)
          |> check_connection_status()

        {:error, conn, _error, responses} ->
          state
          |> State.update_conn(conn)
          |> process_responses(responses)
          |> check_connection_status()
      end
    end

    # A message that is not for the active connection may be for one that is
    # still draining after a GOAWAY.
    defp stream_draining(message, state) do
      found =
        Enum.find_value(state.draining, fn {id, conn} ->
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
    def handle_continue(:process_request_stream_queue, state) do
      {{:value, request}, queue} = :queue.out(state.request_stream_queue)
      {ref, body, _from} = request
      window_size = get_window_size(state.conn, ref)
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
    def terminate(_reason, _state) do
      :normal
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
        {:error, reason} -> cancel_dead_stream(state, request_ref, pid, reason)
      end
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

      conn_id = State.conn_id(state, request_ref)
      state = drop_request(state, request_ref)

      case Mint.HTTP2.cancel_request(conn_for(state, conn_id), request_ref) do
        {:ok, conn} -> put_conn(state, conn_id, conn)
        {:error, conn, _error} -> put_conn(state, conn_id, conn)
      end
    end

    defp drop_request(state, request_ref) do
      {_ref, state} = State.pop_ref(state, request_ref)
      drop_queued_request_chunks(state, request_ref)
    end

    defp drop_queued_request_chunks(state, request_ref) do
      {dropped, kept} =
        state.request_stream_queue
        |> :queue.to_list()
        |> Enum.split_with(&match?({^request_ref, _body, _from}, &1))

      for {_ref, _body, from} <- dropped, not is_nil(from) do
        GenServer.reply(from, {:error, "the request was cancelled"})
      end

      State.update_request_stream_queue(state, :queue.from_list(kept))
    end

    defp chunk_body_and_enqueue_rest({request_ref, body, from}, state) do
      {head, tail} = chunk_body(body, get_window_size(state.conn, request_ref))

      case Mint.HTTP.stream_request_body(state.conn, request_ref, head) do
        {:ok, conn} ->
          queue = :queue.in_r({request_ref, tail, from}, state.request_stream_queue)

          new_state =
            state
            |> State.update_conn(conn)
            |> State.update_request_stream_queue(queue)

          {:noreply, new_state}

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

          {:noreply, State.update_conn(state, conn)}
      end
    end

    defp stream_body_and_reply({request_ref, body, from}, state) do
      send_eof? = is_nil(from)

      case stream_body(state.conn, request_ref, body, send_eof?) do
        {:ok, conn} ->
          if not send_eof? do
            GenServer.reply(from, :ok)
          end

          check_request_stream_queue(State.update_conn(state, conn))

        {:error, conn, error} ->
          if not send_eof? do
            GenServer.reply(from, {:error, error})
          else
            state
            |> State.stream_response_pid(request_ref)
            |> StreamResponseProcess.consume(:error, error)
          end

          check_request_stream_queue(State.update_conn(state, conn))
      end
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

    defp finish_all_pending_requests(state) do
      new_state =
        state.request_stream_queue
        |> :queue.to_list()
        |> Enum.reduce(state, fn {request_ref, _, _} = request, acc_state ->
          case request do
            {ref, _body, nil} ->
              acc_state
              |> State.stream_response_pid(ref)
              |> end_stream_response(@connection_closed_error)

            {ref, _body, from} ->
              acc_state
              |> State.stream_response_pid(ref)
              |> end_stream_response(@connection_closed_error)

              GenServer.reply(from, {:error, @connection_closed_error})
          end

          {_request_data, new_state} = State.pop_ref(acc_state, request_ref)
          new_state
        end)

      # Requests still draining on a connection retired by a GOAWAY are not
      # affected: their connection is a different one.
      clean_state =
        new_state
        |> fail_requests(State.refs_on(new_state, :active), @connection_closed_error)
        |> State.update_request_stream_queue(:queue.new())

      maybe_attempt_reconnect(%{clean_state | draining_in_place: false})
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
    # requests still draining, so fail them instead of leaving callers waiting.
    defp fail_draining(state) do
      Enum.reduce(state.draining, state, fn {id, conn}, acc ->
        Mint.HTTP.close(conn)
        fail_requests(acc, State.refs_on(acc, id), @connection_closed_error)
      end)
      |> Map.put(:draining, %{})
    end

    defp end_stream_response(pid, error) do
      StreamResponseProcess.consume(pid, :error, error)
      StreamResponseProcess.done(pid)
    end

    defp maybe_attempt_reconnect(%{retry: 0} = state) do
      send(state.parent, {:elixir_grpc, :connection_down, self()})
      {:stop, :normal, fail_draining(state)}
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
      {:stop, :normal, fail_draining(state)}
    end

    defp maybe_attempt_reconnect(state) do
      next_attempt = state.retry_attempt + 1

      Logger.info(
        "Attempting reconnection #{next_attempt}/#{state.retry} to #{state.scheme}://#{state.host}:#{state.port}"
      )

      start = System.monotonic_time()

      case Mint.HTTP.connect(state.scheme, state.host, state.port, state.connect_opts) do
        {:ok, conn} ->
          Logger.info("Reconnected successfully to #{state.scheme}://#{state.host}:#{state.port}")

          :telemetry.execute(
            @reconnect_stop_event,
            %{duration: System.monotonic_time() - start},
            reconnect_metadata(state, next_attempt)
          )

          new_state = %{state | conn: conn, retry_attempt: 0}
          {:noreply, new_state}

        {:error, reason} ->
          Logger.warning(
            "Reconnection attempt #{next_attempt}/#{state.retry} failed: #{inspect(reason)}"
          )

          :telemetry.execute(
            @reconnect_error_event,
            %{duration: System.monotonic_time() - start},
            Map.put(reconnect_metadata(state, next_attempt), :reason, reason)
          )

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

    defp check_connection_status(state) do
      cond do
        Mint.HTTP.open?(state.conn, :write) ->
          check_request_stream_queue(state)

        # Readable but not writable: the server sent a GOAWAY. It still answers
        # the requests it accepted, it just takes no new ones on this connection.
        Mint.HTTP.open?(state.conn, :read) ->
          handle_goaway(state)

        true ->
          finish_all_pending_requests(state)
      end
    end

    # A replacement was already tried for this GOAWAY and failed: keep
    # draining; the regular reconnect runs once the server closes the socket.
    defp handle_goaway(%{draining_in_place: true} = state), do: check_request_stream_queue(state)

    # Open a new connection for new requests right away and let the retired
    # one finish the requests in flight on it. This is a planned handover by
    # the server (e.g. grpc-go's MaxConnectionAge), not a failure, so it
    # happens regardless of the :retry setting and does not use its backoff.
    defp handle_goaway(state) do
      {:goaway, error_code, _debug_data} = state.conn.state
      in_flight = State.refs_on(state, :active)

      case Mint.HTTP.connect(state.scheme, state.host, state.port, state.connect_opts) do
        {:ok, new_conn} ->
          emit_goaway(state, length(in_flight), error_code, true)
          id = make_ref()

          state
          # Request bodies still queued can no longer be sent on the old
          # connection, so the server will never answer those requests.
          |> fail_requests(queued_refs(state, in_flight), @connection_closed_error)
          |> then(&drop_queued_chunks(&1, in_flight))
          |> then(&State.move_requests(&1, State.refs_on(&1, :active), id))
          |> Map.update!(:draining, &Map.put(&1, id, state.conn))
          |> State.update_conn(new_conn)
          |> close_if_drained(id)
          |> check_request_stream_queue()

        {:error, reason} ->
          Logger.warning(
            "Server at #{state.scheme}://#{state.host}:#{state.port} is closing the connection " <>
              "and opening a new one failed: #{inspect(reason)}; finishing the requests in flight first"
          )

          emit_goaway(state, length(in_flight), error_code, false)
          check_request_stream_queue(%{state | draining_in_place: true})
      end
    end

    defp queued_refs(state, refs) do
      queued =
        for {ref, _body, _from} <- :queue.to_list(state.request_stream_queue),
            into: MapSet.new(),
            do: ref

      Enum.filter(refs, &MapSet.member?(queued, &1))
    end

    defp drop_queued_chunks(state, refs) do
      Enum.reduce(refs, state, &drop_queued_request_chunks(&2, &1))
    end

    # Once a draining connection has no requests left, or the server closed
    # it, close it and fail whatever was still waiting on it.
    defp close_if_drained(state, :active), do: state

    defp close_if_drained(state, id) do
      case Map.fetch(state.draining, id) do
        :error ->
          state

        {:ok, conn} ->
          refs = State.refs_on(state, id)

          if refs == [] or not Mint.HTTP.open?(conn, :read) do
            Mint.HTTP.close(conn)

            state
            |> fail_requests(refs, @connection_closed_error)
            |> Map.update!(:draining, &Map.delete(&1, id))
          else
            state
          end
      end
    end

    defp emit_goaway(state, in_flight, error_code, reconnected) do
      :telemetry.execute(@goaway_event, %{in_flight: in_flight}, %{
        scheme: state.scheme,
        host: state.host,
        port: state.port,
        error_code: error_code,
        reconnected: reconnected
      })
    end

    defp conn_for(state, :active), do: state.conn
    defp conn_for(state, id), do: Map.fetch!(state.draining, id)

    defp put_conn(state, :active, conn), do: State.update_conn(state, conn)
    defp put_conn(state, id, conn), do: put_in(state.draining[id], conn)

    defp stream_error(%Mint.HTTPError{reason: :unprocessed}), do: @unprocessed_error
    defp stream_error(reason), do: reason

    # Starting a request on a connection the server is closing.
    defp request_error(%Mint.HTTPError{reason: reason})
         when reason in [:closed, :closed_for_writing],
         do: @connection_closed_error

    defp request_error(reason), do: reason
  end
end
