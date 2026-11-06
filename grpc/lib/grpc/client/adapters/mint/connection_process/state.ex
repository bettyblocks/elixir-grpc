if Code.ensure_loaded?(Mint.HTTP) do
  defmodule GRPC.Client.Adapters.Mint.ConnectionProcess.State do
    @moduledoc false

    defstruct [
      :conn,
      :parent,
      :scheme,
      :host,
      :port,
      :connect_opts,
      :retry_timeout_ms,
      requests: %{},
      request_stream_queue: :queue.new(),
      retry: 0,
      retry_attempt: 0,
      # Connections the server retired with a GOAWAY, keyed by an id, kept
      # until the requests still in flight on them are answered.
      draining: %{},
      # Set when the server sent a GOAWAY but opening a replacement failed:
      # `conn` keeps draining and the regular reconnect runs once it closes.
      draining_in_place: false
    ]

    def new(conn, opts) do
      %__MODULE__{
        conn: conn,
        request_stream_queue: :queue.new(),
        parent: opts[:parent],
        scheme: opts[:scheme],
        host: opts[:host],
        port: opts[:port],
        connect_opts: opts[:connect_opts] || [],
        retry: opts[:retry] || 0
      }
    end

    defguard retries_exhausted?(state)
             when state.retry != :infinity and state.retry_attempt >= state.retry

    def update_conn(state, conn) do
      %{state | conn: conn}
    end

    def update_request_stream_queue(state, queue) do
      %{state | request_stream_queue: queue}
    end

    def put_empty_ref_state(state, ref, response_pid) do
      put_in(state.requests[ref], %{
        stream_response_pid: response_pid,
        done: false,
        response: %{},
        conn_id: :active
      })
    end

    @doc """
    Which connection a request runs on: `:active` (`state.conn`) or the id of
    a draining connection in `state.draining`.
    """
    def conn_id(state, ref), do: Map.get(state.requests[ref], :conn_id, :active)

    @doc "The refs of the requests running on the given connection."
    def refs_on(state, conn_id) do
      for {ref, request} <- state.requests,
          Map.get(request, :conn_id, :active) == conn_id,
          do: ref
    end

    @doc "Record that the given requests now run on connection `conn_id`."
    def move_requests(state, refs, conn_id) do
      Enum.reduce(refs, state, fn ref, acc -> put_in(acc.requests[ref][:conn_id], conn_id) end)
    end

    def update_response_status(state, ref, status) do
      put_in(state.requests[ref].response[:status], status)
    end

    def update_response_headers(state, ref, headers) do
      put_in(state.requests[ref].response[:headers], headers)
    end

    def empty_headers?(state, ref) do
      is_nil(state.requests[ref].response[:headers])
    end

    def stream_response_pid(state, ref) do
      state.requests[ref].stream_response_pid
    end

    defguard has_request_ref(state, ref) when is_map_key(state.requests, ref)

    def pop_ref(state, ref) do
      pop_in(state.requests[ref])
    end

    def append_response_data(state, ref, new_data) do
      update_in(state.requests[ref].response[:data], fn data -> (data || "") <> new_data end)
    end
  end
end
