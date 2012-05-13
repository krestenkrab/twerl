-module(stream_client).
-export([
          connect/4,
          handle_connection/2
        ]).

-define(CONTENT_TYPE, "application/x-www-form-urlencoded").
-define(LENGTH_DELIMITED, <<"delimited=length">>).

-record(state, { mode = delimiter :: delimiter|body,
                 prev = <<>>,
                 callback,
                 body_length :: integer()
               }).

-spec connect(string(), list(), string(), fun()) -> ok | {error, reason}.
connect(Url, Headers, PostData, Callback) ->
    case PostData of
        <<>> -> PostData2 = ?LENGTH_DELIMITED;
        _    -> PostData2 = << PostData/binary, $&, (?LENGTH_DELIMITED)/binary >>
    end,
    case catch httpc:request(post, {Url, Headers, ?CONTENT_TYPE, PostData2}, [], [{sync, false}, {stream, self}]) of
        {ok, RequestId} ->
            ?MODULE:handle_connection(RequestId, #state{ mode=delimiter, callback=Callback });

        {error, Reason} ->
            {error, {http_error, Reason}}
    end.

-spec handle_connection(term(), #state{}) -> ok | {error, term()}.
handle_connection(RequestId, State) ->
    receive
        % stream opened
        {http, {RequestId, stream_start, _Headers}} ->
            handle_connection(RequestId, State);

        % stream received data
        {http, {RequestId, stream, Data}} ->
            State2 = handle_data(Data, State),
            handle_connection(RequestId, State2);

        % stream closed
        {http, {RequestId, stream_end, _Headers}} ->
            {ok, RequestId};

        % connected but received error cod
        % 401 unauthorised - authentication credentials rejected
        {http, {RequestId, {{_, 401, _}, _Headers, _Body}}} ->
            {error, unauthorised};

        % 406 not acceptable - invalid request to the search api
        {http, {RequestId, {{_, 406, _}, _Headers, Body}}} ->
            {error, {invalid_params, Body}};

        % connection error
        % may happen while connecting or after connected
        {http, {RequestId, {error, Reason}}} ->
            {error, {http_error, Reason}};

        % received terminate message
        terminate ->
            {ok, RequestId}
    end.

%% waiting for a delimiter, nothing previously received
handle_data(Data, #state{ mode=delimiter, prev= <<>> } = State) ->
    case split_line(Data) of
        noline ->
            State#state{ prev=Data };

        {<<>>, Rest} ->
            handle_data(Rest, State);

        {Line, Rest} ->
            Length = binary_to_integer(Line), % subtract the newline
            handle_data( Rest, State#state{ mode=body, body_length=Length, prev= <<>> })
    end;

%% waiting for a delimiter, somethine received ... just combine and loop
handle_data(NewBin, #state{ mode=delimiter, prev=Prev } = State) ->
    handle_data( << Prev/binary, NewBin/binary >>, State#state{ prev= <<>> });

handle_data(NewBin, #state{ mode=body, body_length=Length, prev=Prev } = State) ->
    case Prev of
        <<>> -> Combined = NewBin;
        _    -> Combined = [NewBin|Prev]
    end,

    case erlang:iolist_size(Combined) >= Length of
        true ->
            <<Body:Length/binary, Rest/binary>> = erlang:iolist_to_binary(Combined),
            Callback = State#state.callback,
            DecodedData = stream_client_util:decode(Body),
            Callback(DecodedData),
            handle_data(Rest, State#state{ mode=delimiter, prev= <<>> });
        false ->
            State#state{ prev=Combined }
    end.

%% @doc parse base10 integer from a binary
binary_to_integer(Bin) ->
    binary_to_integer(0, Bin).

binary_to_integer(N, <<>>) ->
    N;
binary_to_integer(N, <<CH, Rest/binary>>) ->
    binary_to_integer(N*10 + (CH - $0), Rest).

%% @doc Split binary into the first line (up to \r\n or \n), and rest.
%% Line-separator is not included in the Line.
%% -spec split_line(binary()) -> noline | {Line::binary(), Rest::binary()}
split_line(Bin) ->
    case binary:match(Bin, [<<13, 10>>, <<10>>]) of
        nomatch ->
            noline;
	{Pos, 2} ->
            <<Line :Pos /binary, 13, 10, Rest /binary>> = Bin,
            {Line, Rest};
	{Pos, 1} ->
            <<Line :Pos /binary, 10, Rest /binary>> = Bin,
            {Line, Rest}
    end.
