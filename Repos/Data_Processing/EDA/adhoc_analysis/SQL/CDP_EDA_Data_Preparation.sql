-- Convert struct to dataframe

create table tmp_mg_tags_0513 as
select unifyid.id, single_tag.tagid as tagid, single_tag.score as score
from cdp_tags_combine_0513
lateral view explode(tagrecord.tags) adTable as single_tag
where unifyid.type = 'p';