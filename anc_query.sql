with anc_event as(
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
				(em.event_type = 'Update ANC Registration'
				or em.event_type = 'ANC Registration')
				
			)	
			,
			last_anc_event as (
			select
				c.json,
				c.id cid,
				em.id,
				em.last_id,
				cm.base_entity_id,				
				cm.first_name,
				cm.last_name,
				cm.middle_name,
				cm.unique_id,				
				em.json as ejson ,
				em.provider_id,
				em.event_date,
				em.location_id,
				extract(year
				from
				age(current_date, cm.birth_date)) :: int as age_year_part
				
			from
				anc_event em
			join core.client_metadata cm on
				em.base_entity_id = cm.base_entity_id
				join core.client c on c.id = cm.client_id
			where
				em.id = em.last_id ),	
				
			
			obs as(
			select
				id,
				jsonb_array_elements(ejson->'obs') o
			from
				last_anc_event ) ,
			last_obs as(
			select
				id,
				max(to_date((o->'values'->>0), 'dd-mm-yyyy')) filter(
			where
				(o->>'formSubmissionField') = 'edd_note') edd,
				max(to_date((o->'values'->>0), 'dd-mm-yyyy')) filter(
			where
				(o->>'formSubmissionField') = 'last_menstrual_period') lmp,
				max(floor(extract(days from (now() - to_date((o->'values'->>0), 'dd-mm-yyyy') )) / 7)) filter(
			where
				(o->>'formSubmissionField') = 'last_menstrual_period' ) gestAge
			from
				obs
			group by
				id)
				
			select
				cl.first_name,
				cl.last_name,
				cl.middle_name,
				cl.unique_id,
				cl.age_year_part,				
				cl.event_date last_contact_date,
				cl.provider_id, 
cl.location_id,
				cl.json ,
				edd,
				 lmp,
				gestAge gestational_age
			from
				last_obs,
				last_anc_event as cl
			where
				last_obs.id = cl.id 
