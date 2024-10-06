select member.member_first_name, member.member_last_name, 
	status.claim_status, 
	provider.provider_first_name, provider.provider_last_name, provider.network, provider.practice_name, 
	claims_payment.billed_amount, claims_payment.approuved_amount, claims_payment.net_payment
	from 
		((((insurance.member inner join insurance.claims on member.claim_id = claims.claim_id) 
				inner join insurance.status on status.status_id = claims.status_id)
                inner join insurance.provider on provider.claim_id = member.claim_id)
				inner join insurance.claims_payment on claims_payment.claim_id = claims.claim_id)
	where claims_payment.approuved_amount > 2200
	order by claims_payment.approuved_amount desc limit 20;
    
select member.member_first_name, member.member_last_name, member.member_dob, member.gender, member.claim_id,
	   status.claim_status, status.type
	from 
		((insurance.member inner join insurance.claims on member.claim_id = claims.claim_id) 
				inner join insurance.status on status.status_id = claims.status_id)
	where status.claim_status = 'paid'
	order by member.member_first_name;
    

create view top_20_approved_customers as (select member.member_first_name, member.member_last_name, status.claim_status, 
	   provider.provider_first_name, provider.provider_last_name, provider.network,
       provider.practice_name, claims_payment.billed_amount, claims_payment.approuved_amount, claims_payment.net_payment
	from 
		((((insurance.member inner join insurance.claims on member.claim_id = claims.claim_id) 
				inner join insurance.status on status.status_id = claims.status_id)
                inner join insurance.provider on provider.claim_id = member.claim_id)
				inner join insurance.claims_payment on claims_payment.claim_id = claims.claim_id)
	where claims_payment.approuved_amount > 2200
	order by claims_payment.approuved_amount desc limit 20);
    
    select * from top_20_approved_customers;
    



