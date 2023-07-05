-- Active: 1688324568743@@127.0.0.1@33066@friendsurance
CREATE ALGORITHM = UNDEFINED DEFINER = `root` @`%` SQL SECURITY DEFINER VIEW `case_one_result` AS
select
    month(
        `table_one_euro`.`transaction_date`
    ) AS `month`,
    year(
        `table_one_euro`.`transaction_date`
    ) AS `year`,
    count(
        `table_one_euro`.`transaction_id`
    ) AS `number_of_transactions`,
    `table_one_euro`.`country` AS `country`,
    count(
        distinct `table_one_euro`.`customer_id`
    ) AS `number_of_customers`,
    sum(
        `table_one_euro`.`eur_amount`
    ) AS `total_amount_eur`
from `table_one_euro`
where
    month(
        `table_one_euro`.`transaction_date`
    )
group by
    `table_one_euro`.`country`,
    month(
        `table_one_euro`.`transaction_date`
    ),
    year(
        `table_one_euro`.`transaction_date`
    )