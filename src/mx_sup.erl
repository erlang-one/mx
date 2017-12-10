-module(mx_sup).
-behaviour(supervisor).
-author('https://twitter.com/_m_2k').

-compile([{parse_transform, lager_transform}]).
-include("mx.hrl").

-export([start_link/0]).
-export([init/1]).

start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).
init([]) ->
    mx:init(),
    {ok, { {one_for_one, 5, 10}, []} }.
