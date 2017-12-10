-module(mx).
-author('https://twitter.com/_m_2k').

-compile(export_all).
-compile([{parse_transform, lager_transform}]).
-include("mx.hrl").

node_list() -> [ node() | nodes() ].

o() -> observer:start().

schema() ->
    [
        {mx_counter,[{attributes,record_info(fields,mx_counter)},{disc_copies,node_list()}]}
    ].
    
all_schema() -> lists:flatten([ Mod:schema() || Mod <- mx:config(schema,[mx])]).

config(Key,Default) -> application:get_env(?MODULE,Key,Default).

init() ->
    application:start(lager),
    NodeList=[node()|nodes()],
    mnesia:create_schema(NodeList),
    application:start(mnesia),
    
    case ensure_tables(all_schema()) of
        {ok, TableList} ->
            mnesia:wait_for_tables(TableList,mx:config(wait_for_tables,infinity)),
            lager:info("Mnesia backend started, available tables: ~p, nodes: ~p",[TableList,NodeList]),
            ok;
        {error, Reason} ->
            lager:error("Tables not created: ~p",[Reason]),
            exit({error, {tables_not_created,Reason}})
    end.

ensure_tables(SchemeList) ->
    Result = [ case lists:member(Name,mnesia:system_info(tables)) of
            false -> {mnesia:create_table(Name, TabDef), Name};
            true -> {{atomic, ok}, Name}
        end || {Name,TabDef} <- SchemeList ],
    {StatusList, TableList} = lists:unzip(Result),
    case lists:usort(StatusList) of
        [] -> {ok, []};
        [{atomic, ok}] -> {ok, TableList};
        Codes -> {error, Codes -- [{atomic, ok}]}
    end.

recreate_tables(TableList) ->
    Removed = [ begin mnesia:delete_table(T), S end || {T,_}=S <- all_schema(), lists:member(T, TableList) ],
    ensure_tables(Removed).


next_id(Key) -> next_id(Key, 1).
next_id(Key, Incr) -> mnesia:dirty_update_counter(mx_counter, Key, Incr).


next(Key) -> mnesia:dirty_update_counter(mx_counter, Key, 1).
add(Rec) -> mnesia:activity(?context, fun(E) -> mnesia:write(E) end, [Rec], ?access_mode).

read(Tab,Id) ->
    Read = fun() -> case mnesia:read(Tab, Id, read) of
        [E] -> {ok,E};
        [] -> {error,{not_found,{Tab,Id}}}
    end end,
    mnesia:activity(?context, Read, [], ?access_mode).
    
write(Rec) ->
    Write = fun() ->
        mnesia:write(element(1,Rec), Rec, write),
        Rec
    end,
    mnesia:activity(?context, Write, [], ?access_mode).
    
all({Tab,TagField}=TabSpec,{TagTab,TagId,LinksField}=TagSpec) ->
    Read = fun() ->
        case get_links_spec_nt(TagSpec,Tab) of
            {ok,{_,_,0}} -> [];
            {ok,{Head,Last,Count}} ->
                fold({Tab,Head,TagField},{TagTab,TagId},fun(E,Acc) -> [E|Acc] end,[],prev,Count)
        end
    end,
    mnesia:activity(?context, Read, [], ?access_mode).

fold({Tab,undefined,TagField}=TabSpec,{TagTab,TagId}=TagSpec,Fun,Acc,Direction,0) -> Acc;
fold({Tab,Id,TagField}=TabSpec,{TagTab,TagId}=TagSpec,Fun,Acc,Direction,0) -> Acc;
fold({Tab,Id,TagField}=TabSpec,{TagTab,TagId}=TagSpec,Fun,Acc,Direction,Count) ->
    case mnesia:read(Tab, Id, read) of
        [E] ->
            Acc2 = Fun(E,Acc),
            Next = case {Direction,tags_nt(E,TagField,TagSpec)} of
                {prev,{ok,{N,_}}} -> N;
                {next,{ok,{_,N}}} -> N
            end,
            fold(TabSpec,TagSpec,Fun,Acc2,Direction,Count - 1);
        [] ->
            mnesia:abort({record_not_found,{Tab,Id}})
    end.

delete(Tab,Key,LinkList) -> % LinkList :: [{#rec.link,table1},{#rec.tag}]
    ok.

head(TagSpec,LinkTab) ->
    Fun = fun() ->
        case get_links_spec_nt(TagSpec,LinkTab) of
            {ok,{undefined,_,_}} -> {error,empty};
            {ok,{Head,_,_}}      -> {ok,Head};
            {error,Reason}       -> {error,Reason}
        end
    end,
    mnesia:activity(?context, Fun, [], ?access_mode).

last(TagSpec,LinkTab) ->
    Fun = fun() ->
        case get_links_spec_nt(TagSpec,LinkTab) of
            {ok,{_,undefined,_}} -> {error,empty};
            {ok,{_,Last,_}}      -> {ok,Last};
            {error,Reason}       -> {error,Reason}
        end
    end,
    mnesia:activity(?context, Fun, [], ?access_mode).

get_links_spec_nt({TagTab,TagId,LinksField}=TagSpec,LinkTab) ->
    case mnesia:read(TagTab, TagId, read) of
        [E] ->
            case element(LinksField,E) of
                #{ LinkTab := {Head,Last,Count}} -> {ok,{Head,Last,Count}};
                #{} -> {ok,{undefined,undefined,0}};
                _ -> mnesia:abort({malformed_links_spec,[TagSpec,LinkTab]})
            end;
        [] ->
            {error, {not_found,{TagTab,TagId}}}
    end.
    
get_tags_spec_nt({Tab,Id,TagField}=TabSpec,{TagTab,TagId}=TagSpec) ->
    case mnesia:read(Tab, Id, read) of
        [E] -> tags_nt(E,TagField,TagSpec);
        [] -> mnesia:abort({record_not_found,{TagTab,TagId}})
    end.

tags_nt(E,TagField,{TagTab,TagId}=TagSpec) ->
    case element(TagField,E) of
        #{ TagSpec := {Prev,Next}} -> {ok,{Prev,Next}};
        #{} -> {ok,{undefined,undefined}};
        Spec -> mnesia:abort({malformed_tags_spec,[Spec,TagSpec]})
    end.
    
ensure_map(Pos,Record) ->
    case element(Pos,Record) of
        #{}=Map -> Map;
        _ -> #{}
    end.

link({Tab,Key,TagField},{TagTab,TagKey,LinksField}) when Tab =/= TagTab ->
    LinkFun = fun() ->
        case mnesia:read(Tab, Key, read) of
            [E] ->
                TagMap = ensure_map(TagField,E),
                case maps:find({TagTab,TagKey}, TagMap) of
                    error ->
                        case mnesia:read(TagTab, TagKey, read) of
                            [ET] ->
                                ET_LinksMap = ensure_map(LinksField,ET),
                                
                                {ET_Head,E_Prev,ET_Count} = case maps:find(Tab, ET_LinksMap) of
                                    {ok, {Head,Last,Count}} ->
                                        [EP] = mnesia:read(Tab, Head, read),
                                        EP_TagMap = element(TagField,EP),
                                        EP_TagMap2 = maps:update_with({TagTab,TagKey},
                                            fun({Prev,?undef}) -> {Prev,Key} end, EP_TagMap),
                                        EP2 = setelement(TagField,EP,EP_TagMap2),
                                        ok = mnesia:write(Tab,EP2,write),
                                        {Head,Last,Count};
                                    error ->
                                        {?undef,Key,0}
                                end,
                        
                                TagMap2 = maps:put({TagTab,TagKey}, {ET_Head,?undef}, TagMap),
                                E2 = setelement(TagField,E,TagMap2),
                                
                                LinksMap2 = maps:put(Tab,{Key,E_Prev,ET_Count + 1},ET_LinksMap),
                                ET2 = setelement(LinksField,ET,LinksMap2),
                                        
                                ok = mnesia:write(Tab,E2,write),
                                ok = mnesia:write(TagTab,ET2,write);
                            [] ->
                                mnesia:abort({record_tag_not_found,{TagTab,TagKey}})
                        end;
                    {ok,_} ->
                        mnesia:abort({tag_already_exist,{{TagTab,TagKey},TagMap}})
                end;
            [] ->
                mnesia:abort({record_not_found,{Tab,Key}})
        end
    end,
    mnesia:activity(?context, LinkFun, [], ?access_mode).

% Opts: auto_tag - auto remove tag element if link count = 0
unlink({Tab,Key,TagField}=TabSpec,{TagTab,TagKey,LinksField}=TagSpec,Opts) when Tab =/= TagTab ->
    UnlinkExistTag = fun() ->
        case mnesia:read(Tab, Key, read) of
            [E] ->
                TagMap = ensure_map(TagField,E),
                TagMap2 = case {mnesia:read(TagTab,TagKey,read), maps:take({TagTab,TagKey},TagMap)} of
                    {[ET],{{?undef,?undef},TM}} ->
                        case lists:member(auto_tag,Opts) of
                            true ->
                                ok = mnesia:delete(TagTab,TagKey, write);
                            false ->
                                ET_LinksMap = ensure_map(LinksField,ET),
                                ET2_LinksMap = maps:remove(Tab, ET_LinksMap),
                                ET2 = setelement(LinksField,ET,ET2_LinksMap),
                                ok = mnesia:write(TagTab,ET2,write)
                        end,
                        TM;
                    {[ET],{{?undef,Next}=V, TM}} -> shift(TabSpec,TagSpec,ET,{next,Next}), TM;
                    {[ET],{{Prev,?undef}=V, TM}} -> shift(TabSpec,TagSpec,ET,{prev,Prev}), TM;
                    {[ET],{{Prev,Next} = V, TM}} -> weld(TabSpec,TagSpec,ET,V), TM;
                    {[],_} -> mnesia:abort({record_tag_not_found,{TagTab,TagKey}});
                    {_,error} -> mnesia:abort({tag_not_found,{{TagTab,TagKey},TagMap}})
                end,
                
                E2 = setelement(TagField,E,TagMap2),
                ok = mnesia:write(Tab,E2,write);
            [] -> mnesia:abort({record_not_found,{Tab,Key}})
        end
    end,
    mnesia:activity(?context, UnlinkExistTag, [], ?access_mode).

shift({Tab,Key,TagField},{TagTab,TagKey,LinksField},ET,{Position,Link}) ->    
    %%% ET
    ET_LinksMap = ensure_map(LinksField,ET),
    ET2_LinksMap = maps:update_with(Tab, fun({Head,Last,Count}) ->
            case Position of
                next -> {Head,Link,Count - 1};
                prev -> {Link,Last,Count - 1}
            end
        end, ET_LinksMap),
    ET2 = setelement(LinksField,ET,ET2_LinksMap),
    
    %%% EX
    [EX] = mnesia:read(Tab, Link, read),
    EX_TagMap = element(TagField,EX),
    EX_TagMap2 = maps:update_with({TagTab,TagKey},fun
        ({P,N}) when P =:= Key -> {?undef,N}; % warning, match pattern with Key
        ({P,N}) when N =:= Key -> {P,?undef}  % warning, match pattern with Key
     end, EX_TagMap),
    EX2 = setelement(TagField,EX,EX_TagMap2),
    
    ok = mnesia:write(TagTab,ET2,write),
    ok = mnesia:write(Tab,EX2,write).

weld({Tab,Key,TagField},{TagTab,TagKey,LinksField},ET,{LinkA,LinkB}) ->
    %%% ET
    ET_LinksMap = ensure_map(LinksField,ET),
    ET2_LinksMap = maps:update_with(Tab, fun({Head,Last,Count}) ->
            {Head,Last,Count - 1}
        end, ET_LinksMap),
    ET2 = setelement(LinksField,ET,ET2_LinksMap),
    
    
    %%% EX prev + EX next
    [EXA] = mnesia:read(Tab, LinkA, read),
    [EXB] = mnesia:read(Tab, LinkB, read),
    EXA_TagMap = element(TagField,EXA),
    EXB_TagMap = element(TagField,EXB),
    Fun = fun(Map,LinkId) ->
        maps:update_with({TagTab,TagKey},fun
            ({P,N}) when P =:= Key -> {LinkId,N}; % warning, match pattern with Key
            ({P,N}) when N =:= Key -> {P,LinkId}  % warning, match pattern with Key
        end, Map)
    end,
    EXA_TagMap2 = Fun(EXA_TagMap,LinkB),
    EXB_TagMap2 = Fun(EXB_TagMap,LinkA),
    EXA2 = setelement(TagField,EXA,EXA_TagMap2),
    EXB2 = setelement(TagField,EXB,EXB_TagMap2),
    
    ok = mnesia:write(Tab,EXA2,write),
    ok = mnesia:write(Tab,EXB2,write),
    ok = mnesia:write(TagTab,ET2,write).
