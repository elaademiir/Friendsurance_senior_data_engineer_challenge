-- Active: 1688324568743@@127.0.0.1@33066@friendsurance
CREATE ALGORITHM = UNDEFINED DEFINER = `root` @`%` SQL SECURITY DEFINER VIEW `case_two_result` AS
select
    `a`.`to_currency` AS `currency`,
    `a`.`effective_date` AS `today_date`,
    `a`.`rate` AS `today_rate`, (
        `a`.`effective_date` + interval 15 day
    ) AS `next_date`,
    `b`.`next_rate` AS `next_rate`,
    round( ( (`a`.`rate` - `b`.`next_rate`) / `a`.`rate`
        ),
        5
    ) AS `rate_evolution_percentage`
from (
        `friendsurance`.`tbl_exchange_rate` `a`
        join (
            select
                `friendsurance`.`tbl_exchange_rate`.`rate` AS `next_rate`,
                `friendsurance`.`tbl_exchange_rate`.`effective_date` AS `my_day`,
                `friendsurance`.`tbl_exchange_rate`.`to_currency` AS `to_currency`
            from
                `friendsurance`.`tbl_exchange_rate`
        ) `b` on( ( (
                    `a`.`to_currency` = `b`.`to_currency`
                )
                and ( (
                        `a`.`effective_date` + interval 15 day
                    ) = `b`.`my_day`
                )
            )
        )
    )
limit 0, 100