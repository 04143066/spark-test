				SELECT
					x.batch_id					as	batch_id			,
					count(x.parent_sale_ord_id)	as	parent_sale_ord_id	,
					sum(y.sum_ord_amt)			as	sum_ord_amt			,
					sum(y.sum_dq_jq_pay_amt)	as	sum_dq_jq_pay_amt	,
					sum(y.sum_tot_pref_amt)		as	sum_tot_pref_amt
				from
				(
					SELECT	
						parent_sale_ord_id,
						batch_id
						
					FROM	
						gdm.gdm_m07_cps_basic_info
					WHERE
						dt = '2017-10-10'
						AND	parent_sale_ord_id <> 0
				)	x
				join
				(
					select
						par_sale_ord_id								,
						sum(ord_amt)				as	sum_ord_amt		,
						sum(dq_and_jq_pay_amt + pop_shop_dq_pay_amt + pop_shop_jq_pay_amt) as sum_dq_jq_pay_amt	,
						sum(tot_pref_amt)			as	sum_tot_pref_amt
					from
						gdm.gdm_m04_zs_shop_ord_det
					group by
						par_sale_ord_id
				)	y
				ON	
					x.parent_sale_ord_id = y.par_sale_ord_id
				group by
					batch_id