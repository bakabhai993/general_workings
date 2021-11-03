
--1.----------------------------------------------------------------------------------------------------------------------
,encode("data",'hex')  -- converts bytea to string
,decode("data",'hex')  -- converts string to bytea
,bytea2numeric("data") -- converts bytea to numeric
--2.----------------------------------------------------------------------------------------------------------------------
--selecting the transcation data  related to an ERC 20 token :  DefiPulse Index token.
--we are working on the polygon chain.
SELECT contract_address
     , topic1 AS method_identifier
     , CONCAT('0x',RIGHT(encode("topic2",'hex'),40)) AS "from"
     , CONCAT('0x',RIGHT(encode("topic3",'hex'),40)) AS "to" 
     , bytea2numeric("data")/1E18 AS amount
  FROM polygon."logs"
  WHERE 1=1
  -- identifier for the transaction/transfer method call.
  AND "topic1" = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
   -- this is the filter for the DefiPulse Index(contract address).
  AND contract_address = '\x85955046df4668e1dd369d2de9f3aeb98dd2a369'
  --to reduce the amount of data on which the code will run.
  AND block_time > now() - interval '2 weeks'
  ;
--3.----------------------------------------------------------------------------------------------------------------------
--DPI ending balance in a given duration.
WITH 
    transfers_in as 
    (
        SELECT CONCAT('\x',RIGHT(encode("topic3",'hex'),40))::bytea as "address"
             , SUM(bytea2numeric("data"))/1e18 as amount_in 
         FROM polygon."logs"
        WHERE "topic1" = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
          AND "contract_address" = '\x85955046df4668e1dd369d2de9f3aeb98dd2a369'
          AND "block_time" > now() - interval '1 month'
     GROUP BY 1
    ),
    
    transfers_out as 
    (
        SELECT CONCAT('\x',RIGHT(encode("topic2",'hex'),40))::bytea as "address"
             , SUM(bytea2numeric("data"))/1e18 as amount_out 
         FROM polygon."logs"
        WHERE "topic1" = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND "contract_address" = '\x85955046df4668e1dd369d2de9f3aeb98dd2a369'
        AND "block_time" > now() - interval '1 month'
   GROUP BY 1 
    ),
    
    ending_balance as 
    (
        --I want addresses and their total amount_out - total amount_in, and if they have > 0 then they are holding some balance of the token. 
        SELECT t_in."address", amount_in - COALESCE(amount_out, 0) as "balance" FROM "transfers_in" t_in 
        LEFT JOIN "transfers_out" t_out ON t_in."address"=t_out."address"
    ),
    
    --2. order by and get top 10
     positive_balance as (
        SELECT * FROM ending_balance
        WHERE balance > 0
        ORDER BY "balance" DESC
        LIMIT 10 
     )
--4.----------------------------------------------------------------------------------------------------------------------
--Determining whether transfers are being called by a contract or interacted with by a user.
-- this is done by labeling the addresses as a "user" or "contract" .
-- if the address is of type "create" this means it interacted with the opcode thus a contract, this info can be found in the dataset ethereum."traces"

    SELECT pb.*,  CASE WHEN "type" = 'create' AND tr."success" = 'true' THEN 'contract' ELSE 'user' END AS "address_type"
    FROM positive_balances pb 
      LEFT JOIN ethereum."traces" tr ON tr."address" = pb."address"
    ORDER BY pb."balance" DESC


    OR


    SELECT COUNT(address ),COUNT(DISTINCT address)
  FROM polygon."traces" tr 
  WHERE "type" = 'create' AND "success" = 'true'
  ;
--5.----------------------------------------------------------------------------------------------------------------------



