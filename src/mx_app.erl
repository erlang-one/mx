-module(mx_app).
-behaviour(application).
-author('https://twitter.com/_m_2k').

-export([start/2, stop/1]).
-compile(export_all).
-compile([{parse_transform, lager_transform}]).

-include("mx.hrl").

start(_StartType, _StartArgs) -> mx_sup:start_link().
stop(_State) -> ok.
