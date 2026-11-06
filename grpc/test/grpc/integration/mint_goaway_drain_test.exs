defmodule GRPC.Integration.MintGoawayDrainTest do
  # The Mint client after a server GOAWAY (`:max_connection_age` on the server): the
  # connection process opens a new connection at once and sends new calls there, while
  # open calls finish on the old one. This works the same with and without the pool.
  # async: false, because it toggles `:pool_enabled` and uses global telemetry handlers.
  use GRPC.Integration.TestCase, async: false

  @ceiling 5_000
  # Long enough that a new connection does not age out while a test runs.
  @age 3_000
  @adapter GRPC.Client.Adapters.Mint

  @server_opts [
    adapter_opts: [
      max_connection_age: @age,
      max_connection_age_grace: 10_000,
      goaway_initial_timeout: 50
    ]
  ]

  defmodule DrainServer do
    use GRPC.Server, service: Routeguide.RouteGuide.Service

    # latitude 1: tells the test the RPC is open, then waits until the test releases it.
    def get_feature(%{latitude: 1} = point, _stream) do
      send(test_pid(), {:in_flight, self()})

      receive do
        :release -> %Routeguide.Feature{location: point, name: "released"}
      end
    end

    def get_feature(point, _stream), do: %Routeguide.Feature{location: point, name: "quick"}

    # Waits for the end of the request stream, which a client cannot send during a drain.
    def record_route(req_enum, _stream) do
      send(test_pid(), {:in_flight, self()})
      %Routeguide.RouteSummary{point_count: Enum.count(req_enum)}
    end

    defp test_pid, do: :persistent_term.get({GRPC.Integration.MintGoawayDrainTest, :test})
  end

  setup do
    :persistent_term.put({__MODULE__, :test}, self())
    previous_pool = Application.get_env(:grpc, :pool_enabled)
    test_pid = self()
    handler_id = {__MODULE__, make_ref()}

    :telemetry.attach_many(
      handler_id,
      for(event <- [:connected, :streams, :stopped], do: [:grpc, :client, :transport, event]),
      fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      Application.put_env(:grpc, :pool_enabled, previous_pool)
      :persistent_term.erase({__MODULE__, :test})
    end)
  end

  for pool <- [true, false], retry <- [0, 3] do
    describe "pool_enabled: #{pool}, retry: #{retry}" do
      setup do
        Application.put_env(:grpc, :pool_enabled, unquote(pool))
      end

      test "a call that starts in the drain uses a new connection, and the open call finishes on the old one" do
        run_server(
          DrainServer,
          fn port ->
            ref = make_ref()

            {:ok, channel} =
              GRPC.Stub.connect("127.0.0.1:#{port}",
                name: ref,
                adapter: @adapter,
                adapter_opts: [retry: unquote(retry)]
              )

            transport = assert_connected(ref)

            task =
              Task.async(fn ->
                Routeguide.RouteGuide.Stub.get_feature(channel, point(1), timeout: 10_000)
              end)

            assert_receive {:in_flight, rpc}, @ceiling
            [old_server_conn] = :ranch.procs(inspect(DrainServer), :connections)
            server_ref = Process.monitor(old_server_conn)

            # The same connection process opens the replacement when the GOAWAY arrives.
            assert_handed_over(ref, transport)

            assert {:ok, %Routeguide.Feature{name: "quick"}} =
                     Routeguide.RouteGuide.Stub.get_feature(channel, point(2), timeout: @ceiling)

            if unquote(pool) do
              assert [%{draining: false, channel: %{adapter_payload: %{conn_pid: ^transport}}}] =
                       channels(channel)
            end

            assert Process.alive?(rpc)
            refute_received {:DOWN, ^server_ref, :process, ^old_server_conn, _}
            send(rpc, :release)
            assert {:ok, %Routeguide.Feature{name: "released"}} = Task.await(task, @ceiling)

            # The client closes the old connection once its last call is answered.
            assert_receive {:DOWN, ^server_ref, :process, ^old_server_conn, _}, @ceiling

            # Nothing went down: no stop, and no further connection.
            refute_receive {:telemetry, [:grpc, :client, :transport, :stopped], _,
                            %{logical_connection_ref: ^ref}},
                           300

            refute_received {:telemetry, [:grpc, :client, :transport, :connected], _,
                             %{logical_connection_ref: ^ref}}

            GRPC.Stub.disconnect(channel)
          end,
          0,
          @server_opts
        )
      end
    end
  end

  describe "with the pool" do
    setup do
      Application.put_env(:grpc, :pool_enabled, true)
    end

    test "a client-streaming call cannot send in the drain, and fails with UNAVAILABLE" do
      run_server(
        DrainServer,
        fn port ->
          ref = make_ref()

          {:ok, channel} =
            GRPC.Stub.connect("127.0.0.1:#{port}", name: ref, adapter: @adapter)

          transport = assert_connected(ref)

          stream = Routeguide.RouteGuide.Stub.record_route(channel, timeout: 10_000)
          stream = GRPC.Stub.send_request(stream, point(1))
          assert_receive {:in_flight, _rpc}, @ceiling
          [old_server_conn] = :ranch.procs(inspect(DrainServer), :connections)
          server_ref = Process.monitor(old_server_conn)

          assert_handed_over(ref, transport)

          stream = GRPC.Stub.send_request(stream, point(2))
          stream = GRPC.Stub.end_stream(stream)

          assert {:error, %GRPC.RPCError{status: 14}} = GRPC.Stub.recv(stream)

          # Without the cancel, the call stays open until `:max_connection_age_grace`.
          assert_receive {:DOWN, ^server_ref, :process, ^old_server_conn, _}, @ceiling

          assert {:ok, %Routeguide.Feature{name: "quick"}} =
                   Routeguide.RouteGuide.Stub.get_feature(channel, point(2), timeout: @ceiling)

          GRPC.Stub.disconnect(channel)
        end,
        0,
        @server_opts
      )
    end
  end

  describe "a dead connection process" do
    setup do
      Application.put_env(:grpc, :pool_enabled, true)
    end

    test "a client-streaming call fails with UNAVAILABLE and does not wait forever" do
      run_server(DrainServer, fn port ->
        {:ok, channel} = GRPC.Stub.connect("127.0.0.1:#{port}", adapter: @adapter)

        stream = Routeguide.RouteGuide.Stub.record_route(channel, timeout: 10_000)
        stream = GRPC.Stub.send_request(stream, point(1))

        conn_pid = stream.channel.adapter_payload.conn_pid
        monitor = Process.monitor(conn_pid)
        Process.exit(conn_pid, :kill)
        assert_receive {:DOWN, ^monitor, :process, ^conn_pid, _}, @ceiling

        stream = GRPC.Stub.send_request(stream, point(2))
        stream = GRPC.Stub.end_stream(stream)

        task = Task.async(fn -> GRPC.Stub.recv(stream) end)

        assert {:ok, {:error, %GRPC.RPCError{status: 14}}} = Task.yield(task, @ceiling)

        GRPC.Stub.disconnect(channel)
      end)
    end
  end

  describe "a connection process that stops or dies" do
    setup do
      Application.put_env(:grpc, :pool_enabled, true)
    end

    test "a late send does not replace a reply that already ended" do
      run_server(DrainServer, fn port ->
        {:ok, channel} = GRPC.Stub.connect("127.0.0.1:#{port}", adapter: @adapter)

        stream = Routeguide.RouteGuide.Stub.record_route(channel, timeout: 10_000)
        stream = GRPC.Stub.send_request(stream, point(1), end_stream: true)
        conn_pid = stream.channel.adapter_payload.conn_pid

        # The server has answered when the connection process has no open request.
        await_until(fn -> :sys.get_state(conn_pid).requests == %{} end)
        monitor = Process.monitor(conn_pid)
        GenServer.stop(conn_pid, :normal)
        assert_receive {:DOWN, ^monitor, :process, ^conn_pid, _}, @ceiling

        stream = GRPC.Stub.send_request(stream, point(2))
        stream = GRPC.Stub.end_stream(stream)

        task = Task.async(fn -> GRPC.Stub.recv(stream) end)

        assert {:ok, {:ok, %Routeguide.RouteSummary{point_count: 1}}} =
                 Task.yield(task, @ceiling)

        GRPC.Stub.disconnect(channel)
      end)
    end

    test "a connection process killed during a send fails the call with UNAVAILABLE" do
      run_server(DrainServer, fn port ->
        {:ok, channel} = GRPC.Stub.connect("127.0.0.1:#{port}", adapter: @adapter)

        stream = Routeguide.RouteGuide.Stub.record_route(channel, timeout: 10_000)
        stream = GRPC.Stub.send_request(stream, point(1))
        conn_pid = stream.channel.adapter_payload.conn_pid

        # The suspended process holds the send in its mailbox. The kill ends the call.
        :ok = :sys.suspend(conn_pid)

        on_exit(fn ->
          if Process.alive?(conn_pid), do: :sys.resume(conn_pid)
        end)

        send_task = Task.async(fn -> GRPC.Stub.send_request(stream, point(2)) end)

        await_until(fn -> send_queued?(conn_pid) end)
        Process.exit(conn_pid, :kill)

        assert {:ok, %GRPC.Client.Stream{} = stream} = Task.yield(send_task, @ceiling)
        stream = GRPC.Stub.end_stream(stream)
        task = Task.async(fn -> GRPC.Stub.recv(stream) end)

        assert {:ok, {:error, %GRPC.RPCError{status: 14}}} = Task.yield(task, @ceiling)

        GRPC.Stub.disconnect(channel)
      end)
    end
  end

  defp point(latitude), do: %Routeguide.Point{latitude: latitude, longitude: 0}

  defp assert_connected(ref) do
    assert_receive {:telemetry, [:grpc, :client, :transport, :connected], _,
                    %{logical_connection_ref: ^ref, transport_ref: transport_ref}},
                   @ceiling

    transport_ref
  end

  # The connection process reconnected in place: same process, next generation.
  defp assert_handed_over(ref, transport) do
    assert_receive {:telemetry, [:grpc, :client, :transport, :connected], %{generation: 2},
                    %{logical_connection_ref: ^ref, transport_ref: ^transport, reconnect: true}},
                   @ceiling
  end

  defp channels(channel) do
    [{pool, _}] =
      Registry.lookup(GRPC.Client.Pool.Registry, {GRPC.Client.Pool.Server, channel.pool})

    pool |> :sys.get_state() |> Map.fetch!(:channels) |> Map.values()
  end

  defp send_queued?(pid) do
    {:messages, messages} = Process.info(pid, :messages)
    Enum.any?(messages, &match?({:"$gen_call", _from, {:stream_body, _ref, _body}}, &1))
  end

  # There is no event for these: a bounded poll of process state.
  defp await_until(check, deadline \\ System.monotonic_time(:millisecond) + @ceiling) do
    cond do
      check.() ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("the condition did not hold in time")

      true ->
        Process.sleep(10)
        await_until(check, deadline)
    end
  end
end
