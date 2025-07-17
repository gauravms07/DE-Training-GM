start transaction;
update accounts set balance=balance-200 where acc_id=1;
select sleep(10);

update accounts set balance=balance-100 where acc_id=1;
commit;
