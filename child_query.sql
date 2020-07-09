with child_event as(
		select
			e.json,
			em.event_type,
			em.location_id,
			em.provider_id,
			em.event_date,
			em.base_entity_id,
			em.id,
			max(em.id) over(partition by em.base_entity_id) last_id
		from
			core.event_metadata em
		join core."event" e on
			em.id = e.id
		where
			(em.event_type = 'Child Registration'
			or em.event_type = 'Update Child Registration'
			or em.event_type = 'PNC Child Registration')
				
			)
			,
		last_event as(
		select
			ce.id,
			ce.last_id,
			cm.base_entity_id,
			ce.event_type,
			cm.first_name,
			cm.unique_id,
			cm.last_name,
			cm.middle_name,
			cm.relational_id,
			cm.birth_date,
			ce.json as ejson ,
			ce.provider_id,
			ce.event_date,
			ce.location_id
		from
			child_event ce
		join core.client_metadata cm on
			ce.base_entity_id = cm.base_entity_id
		where
			ce.id = ce.last_id ) 
		
		select
			c.json,
			c.id,
			le.provider_id,
			le.event_date last_contact_date,
			le.location_id,
			le.base_entity_id,
			le.first_name,
			le.last_name,
			le.middle_name,
			le.birth_date,
			le.unique_id,
			le.relational_id,
			c."json"->>'gender' gender,
			extract(year
		from
			age(current_date, le.birth_date)) :: int as age_year_part 
		
			
		from
			last_event le
		join core.client as c on
			c.json->>'baseEntityId' = le.base_entity_id
