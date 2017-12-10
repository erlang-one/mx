-module(mx_schema_definition).
-author('https://twitter.com/_m_2k').


% -export([behaviour_info/1]).

-callback schema() ->  [{atom(),[atom()]}].

% behaviour_info(callbacks) -> [schema/1];
% behaviour_info(_) -> undefined.
