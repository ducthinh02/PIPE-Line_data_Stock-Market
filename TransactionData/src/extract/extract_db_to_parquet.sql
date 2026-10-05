
SELECT 

    industry_name as industry_of_company, 

    company_update_time_stamp ,

    ticker_company ,
    founded_date ,
    charter_capital,
    number_of_employees ,
    exchange_name as exchange_of_company ,
    company_type ,
    listing_date ,
    listing_price ,                            
    listed_volume ,                       
    outstanding_shares,
    ceo_name
FROM COMPANIES c
LEFT JOIN industries i ON c.industry_id = i.industry_id

LEFT JOIN EXCHANGE e ON c.exchange_id = e.exchange_id

WHERE 
    c.company_update_time_stamp >= CURRENT_DATE
    AND c.company_update_time_stamp < CURRENT_DATE + INTERVAL '1 day' ;

SELECT 
    index_code,
    index_name,
    index_description,
    group_name
FROM CK_INDEX ci
LEFT JOIN INDEX_GROUPS ig ON ci.index_group_id = ig.index_group_id
WHERE
    ci.ck_index_update_time_stamp >= CURRENT_DATE
    AND ci.ck_index_update_time_stamp < CURRENT_DATE + INTERVAL '1 day';