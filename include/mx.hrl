-ifndef(DB_HRL).
-define(DB_HRL, "mx.hrl").

% -define(state_table,mx_state).

-define(undef,undefined).
-define(context,{transaction, infinity}).
-define(access_mode,mnesia_frag).

-record(mx_counter,{key, num}).
    
% -define(mx_tag_spec, id,head,last,count = 0).
% -record(mx_tag_spec, {?mx_tag_spec}).

-endif.
