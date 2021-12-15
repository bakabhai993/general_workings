
dune master code file
--1.----------------------------------------------------------------------------------------------------------------------
/*
sample addresses:
0x880dece2ac6aca85c14f7e7f16b3f0a4b59bc3a0
0x41449BD924030deD1e074d454FE354d8e562fcF1
0xFf8D58f85a4f7199c4b9461F787cD456Ad30e594
*/

--Misc bits of code
,encode("data",'hex')  -- converts bytea to string
,decode("data",'hex')  -- converts string to bytea
,bytea2numeric("data") -- converts bytea to numeric
,CONCAT('\x',RIGHT('{{address:}}',40))::bytea --input the address starting with 0x, converts to bytea 
,CONCAT('\x', substring('{{contract_address}}' FROM 3))::bytea --input the address starting with 0x, converts to bytea 
,SELECT generate_series('2021-04-19'::timestamp, date_trunc('day', now()), '1 day') AS dater


SELECT labels.get('\xFf8D58f85a4f7199c4b9461F787cD456Ad30e594');
SELECT labels.get('\xFf8D58f85a4f7199c4b9461F787cD456Ad30e594','ens name');
SELECT labels.get('\\','ens name','activity');
/*
dapp usage
activity
ens name
contract_name
project
eth2 actions
lp_pool_name
balancer_pool
owner
balancer_v2_pool
*/



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
WITH 
tbl_filtered_transaction_hashes
AS
(
    SELECT DISTINCT "hash" 
      FROM ethereum."transactions"
      WHERE "from" = '\x880DEce2Ac6AcA85C14f7e7f16b3f0a4B59Bc3A0' OR "to" = '\x880DEce2Ac6AcA85C14f7e7f16b3f0a4B59Bc3A0'
)
,
tbl_raw_transactions
AS
(
  SELECT e."symbol"
       , l."contract_address" AS token_address
       , (-1)*bytea2numeric("data")/10^(e.decimals) AS amount
       --, "topic1" AS method_identifier
       , decode(RIGHT(encode("topic2",'hex'),40),'hex') AS "address"
       
    FROM ethereum."logs" l
      JOIN tbl_filtered_transaction_hashes h ON (l."tx_hash" = h."hash")
      JOIN erc20.tokens e ON (l."contract_address" = e."contract_address")
    WHERE 1=1
      --AND l."tx_hash" = '\x1aaf5830ddcc34f39059da44c2f20d5cad14f899b3fdf6b569b4cc2b164c7a7b'
      -- identifier for the transaction/transfer method call.
      AND l."topic1" = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'         
      AND decode(RIGHT(encode("topic2",'hex'),40),'hex') = '\x880DEce2Ac6AcA85C14f7e7f16b3f0a4B59Bc3A0'

  UNION ALL

  SELECT e."symbol"
       , l."contract_address" AS token_address
       , ( 1)*bytea2numeric("data")/10^(e.decimals) AS amount
       --, "topic1" AS method_identifier
       , decode(RIGHT(encode("topic3",'hex'),40),'hex') AS "address"
       
    FROM ethereum."logs" l
      JOIN tbl_filtered_transaction_hashes h ON (l."tx_hash" = h."hash")
      JOIN erc20.tokens e ON (l."contract_address" = e."contract_address")
    WHERE 1=1
      --AND l."tx_hash" = '\x1aaf5830ddcc34f39059da44c2f20d5cad14f899b3fdf6b569b4cc2b164c7a7b'
      -- identifier for the transaction/transfer method call.
      AND l."topic1" = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'    
      AND decode(RIGHT(encode("topic3",'hex'),40),'hex') = '\x880DEce2Ac6AcA85C14f7e7f16b3f0a4B59Bc3A0'     
)
SELECT symbol
     , token_address
     , address
     , SUM(amount) AS balance
  FROM tbl_raw_transactions
  GROUP BY symbol, token_address, address
;




  WITH tbl_transactions
  AS
  (
      SELECT e."symbol"
           , l."contract_address" AS token_address
           , (-1)*bytea2numeric("data")/10^(e.decimals) AS amount
           --, "topic1" AS method_identifier
           , decode(RIGHT(encode("topic2",'hex'),40),'hex') AS "address"
           , l.block_time

           
        FROM ethereum."logs" l
          JOIN erc20.tokens e ON (l."contract_address" = e."contract_address")
        WHERE 1=1
          --AND l."tx_hash" = '\x70eb3ce8710055b55e62d4b6b57780e04655a5e8dcf61f340b3ad54f1a4e7d79'
          -- identifier for the transaction/transfer method call.
          AND l."topic1" = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'         
          AND decode(RIGHT(encode("topic2",'hex'),40),'hex') = '\x41449BD924030deD1e074d454FE354d8e562fcF1'
          AND l.contract_address = '\xfdb29741f239a2406ae287913ef12415160378d3'
    
      UNION ALL
    
      SELECT e."symbol"
           , l."contract_address" AS token_address
           , ( 1)*bytea2numeric("data")/10^(e.decimals) AS amount
           --, "topic1" AS method_identifier
           , decode(RIGHT(encode("topic3",'hex'),40),'hex') AS "address"
           , l.block_time
           
        FROM ethereum."logs" l
          JOIN erc20.tokens e ON (l."contract_address" = e."contract_address")
        WHERE 1=1
          --AND l."tx_hash" = '\x70eb3ce8710055b55e62d4b6b57780e04655a5e8dcf61f340b3ad54f1a4e7d79'
          -- identifier for the transaction/transfer method call.
          AND l."topic1" = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'    
          AND decode(RIGHT(encode("topic3",'hex'),40),'hex') = '\x41449BD924030deD1e074d454FE354d8e562fcF1'     
          AND l.contract_address = '\xfdb29741f239a2406ae287913ef12415160378d3'

  )
SELECT * 
  FROM  tbl_transactions
  ORDER BY block_time DESC
;


--6.----------------------------------------------------------------------------------------------------------------------
--getting prices in usd
LEFT JOIN prices.usd p 
ON p.minute = date_trunc('minute', evt_block_time)
AND event."asset" = p.contract_address

--dm if if it is still not working
--this piece of code essentially gives me the last updated price /row in the prices.usd table
LEFT JOIN 
(
    SELECT contract_address, minute, price, decimals
    FROM ( 
            SELECT *, row_number() OVER(PARTITION BY contract_address ORDER BY minute DESC) AS ranker FROM prices."usd" p WHERE minute > now() - interval '7 days'
         ) a
    WHERE ranker = 1

) AS usd
;


--7.----------------------------------------------------------------------------------------------------------------------
--ENS domain, owner mapping
--see this query as well: 
--https://dune.xyz/queries/230953
   SELECT  MIN(CONCAT(name,'.eth')) AS domain  
         , CONCAT('0x',RIGHT(owner::text,40)) AS owner_text
         , owner AS owner_bytea
     FROM ethereumnameservice.view_registrations 
   WHERE owner = '\xFf8D58f85a4f7199c4b9461F787cD456Ad30e594'
 GROUP BY owner
 ;

--8.----------------------------------------------------------------------------------------------------------------------
--check if a contract is decoded on dune or not
 SELECT namespace, name, address, base, dynamic, updated_at, created_at, abi, code 
  FROM ethereum.contracts
WHERE address = (CONCAT('\x', substring('{{contract_address}}' FROM 3))::bytea)
;

SELECT namespace, name, address, base, dynamic, updated_at, created_at, abi, code 
  FROM polygon.contracts
WHERE address = (CONCAT('\x', substring('{{contract_address}}' FROM 3))::bytea)
;

SELECT namespace, name, address, base, dynamic, updated_at, created_at, abi, code 
FROM bsc."contracts"
where address = (CONCAT('\x', substring('{{contract_address}}' FROM 3))::bytea)
;

--9.----------------------------------------------------------------------------------------------------------------------
CASE WHEN COALESCE(token_a_symbol,token_a_address::text) < COALESCE(token_b_symbol,token_b_address::text)
             THEN COALESCE(token_a_symbol,token_a_address::text)||'-'|| COALESCE(token_b_symbol,token_b_address::text)
             ELSE COALESCE(token_b_symbol,token_b_address::text)||'-'|| COALESCE(token_a_symbol,token_a_address::text)
             END AS trading_pair

--10.----------------------------------------------------------------------------------------------------------------------
SELECT 
    block_time, 
    success, 
    gas_price/10^9 AS gas_prices, 
    gas_used,
    (gas_price*gas_used)/10^18 AS eth_paid_for_tx,
    hash
FROM ethereum.transactions


--11.----------------------------------------------------------------------------------------------------------------------
--prices from dex table
WITH trades_with_usd_amount AS 
(
    SELECT usd_amount/(token_a_amount_raw/10^18)  AS price
         , block_time
         , token_a_address AS contract_address
      FROM dex.trades
      WHERE token_a_address = '\x6100dd79fcaa88420750dcee3f735d168abcb771'
        AND category = 'DEX'
        AND token_a_amount_raw > 0
        AND usd_amount IS NOT NULL
    
    UNION ALL
    
    SELECT usd_amount/(token_b_amount_raw/10^18)  AS price
         , block_time
         , token_a_address AS contract_address
      FROM dex.trades
      WHERE token_b_address = '\x6100dd79fcaa88420750dcee3f735d168abcb771'
      AND category = 'DEX'
      AND token_b_amount_raw > 0
      AND usd_amount IS NOT NULL
)
    SELECT
          date_trunc('hour', block_time) as hour
        , contract_address
        , (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)) AS median_price
        , count(1) AS sample_size
    FROM trades_with_usd_amount
    GROUP BY date_trunc('hour', block_time), contract_address
    ORDER BY date_trunc('hour', block_time) DESC, contract_address
    ;




