with family_event as (
select
	em.*,
	max(em.id) over(partition by em.base_entity_id) last_id
from
	core.event_metadata em
where
	em.entity_type = 'ec_family' ),
distinct_family_event as (
select
	fe.provider_id,
	fe.location_id,
	fe.event_date,
	fe.entity_type,
	fe.event_type,
	cm.relational_id,
	cm.base_entity_id,
	cm.unique_id,
	cm.first_name family_first_name,
	cm.last_name family_last_name
from
	family_event fe
join core.client_metadata cm
		using(base_entity_id)
where
	fe.id = fe.last_id ) ,
family_head as (
select
	cm.relational_id family_base_entity_id,
	cm.base_entity_id family_head_base_entity_id,
	cm.first_name family_head_first_name,
	cm.last_name family_head_last_name
from
	core.client_metadata cm
where
	cm.base_entity_id = any (
	select
		relational_id
	from
		distinct_family_event) ) ,
family_head_event as (
select
	e.json ejson,
	em.base_entity_id,
	em.id,
	max(em.id) over(partition by em.base_entity_id) last_id
from
	core.event_metadata em
join core."event" e on
	em.id = e.id
where
	em.base_entity_id = any (
	select
		family_head_base_entity_id
	from
		family_head ) ) ,
family_head_last_event as (
select
	id,
	ejson,
	base_entity_id
from
	family_head_event fhe
where
	id = last_id ),
obs as (
select
	id,
	base_entity_id,
	jsonb_array_elements(ejson->'obs') o
from
	family_head_last_event ) ,
phoneNumber as(
select
	id,
	max(base_entity_id) base_entity_id,
	max(o->'values'->>0) filter(
where
	(o->>'formSubmissionField') = 'phone_number') phone_number
from
	obs
group by
	id) ,
family_and_family_head_and_phone as(
select
	pn.phone_number,
	dfe.family_first_name,
	dfe.family_last_name ,
	dfe.base_entity_id,
	dfe.unique_id,
	fh.family_head_first_name,
	fh.family_head_last_name,
	fh.family_head_base_entity_id relational_id ,
	concat(fh.family_head_first_name, ' ', fh.family_head_last_name) as family_head,
	dfe.provider_id,
	dfe.location_id
from
	family_head fh
join distinct_family_event dfe on
	fh.family_head_base_entity_id = dfe.relational_id
join phoneNumber pn on
	fh.family_head_base_entity_id = pn.base_entity_id ),
member_count as (
select
	relational_id,
	count(*) as member_count
from
	core.client_metadata cm
where
	relational_id = any (
	select
		base_entity_id
	from
		family_and_family_head_and_phone)
group by
	relational_id ),
family_and_family_head_and_phone_member_count as(
select
	ffhp.*,
	member_count,
	c."json",
	c."json"->>'dateCreated' ,
	c.date_deleted,
	c.id
from
	family_and_family_head_and_phone ffhp
join member_count mc on
	ffhp.base_entity_id = mc.relational_id
join core.client c on
	c."json"->>'baseEntityId' = ffhp.base_entity_id )
select
	*
from
	family_and_family_head_and_phone_member_count


