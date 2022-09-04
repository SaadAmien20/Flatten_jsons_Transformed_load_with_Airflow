with Get_Dublicate_Rows
as
(
select 
	Row_Number() 
	over(partition by Division,Business,Platform,Source,Key_1,Key_value_1,Key_2,Key_value_2 order by row_rule_id)RN,
	Row_Number()  over(partition by row_rule_id order by row_rule_id desc)row_rule_id_Rn,
	Division,
	Business,
	Platform,
	Country,
	Metric,
	Source,
	Key_1,
	Key_value_1,
	Key_2,
	Key_value_2,
	Partner,
	Channel,
	row_rule_id,
	Created_at,
	Is_conditional
from 
	mapping_table

)
select 
 ROW_NUMBER() over(order by row_rule_id desc)Row,
	Division,
	Business,
	Platform,
	Country,
	Metric,
	Source,
	Key_1,
	Key_value_1,
	Key_2,
	Key_value_2,
	Partner,
	Channel,
	row_rule_id,
	Created_at,
	Is_conditional
from 
	Get_Dublicate_Rows
	where RN>1 and row_rule_id_Rn = 1
	