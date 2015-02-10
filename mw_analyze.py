#!/usr/bin/python

# Copyright (c) 2015 by Matthew Cross
#
# A script to do some analysis of MoneyWell application data files.
# It is intended to help diagnose issues where account balances and
# bucket balances don't line up.
#
# It starts by printing the account balances and bucket balances, and
# comparing them.  It will also check the initial bucket balances
# against the initial account balances on that date to ensure they all
# match up, and if not it will print out a message describing that
# discrepancy.  It then proceeds to look for and print out any
# transactions that look like they could cause the imbalance.
# Transactions that it looks for include:
#
## Transactions with buckets assign in an account that is excluded
## from bucket balances.
#
## Transactions without buckets assigned in accounts that are included
## in bucket balances.
#
## Transfers between "bucketed accounts" that have buckets assigned on
## one side but not the other.
#
## Transfers between "bucketed" and "non-bucketed" accounts that do
## not have buckets assigned appropriately (bucket assigned in
## bucketed account, no bucket in non-bucketed account side).
#
## Split transactions that don't add up or have split amounts that do
## not have buckets assigned (in bucketed accounts).

import sqlite3
import datetime
import os

# Describes an account
class Account:
    def __init__(self, key, name, bucketed):
        self.key = key
        self.name = name
        self.bucketed = bucketed

    def __repr__(self):
        isbucketed = ''
        if self.bucketed:
            isbucketed = ' (bucketed)'
        return 'account %d: %s%s' % (self.key, self.name, isbucketed)

# Describes a bucket
class Bucket:
    def __init__(self, key, name, hidden):
        self.key = key
        self.name = name
        self.hidden = hidden

    def __repr__(self):
        ishidden = ''
        if self.hidden:
            ishidden = ' (hidden)'
        return 'bucket %d: %s%s' % (self.key, self.name, ishidden)

# Converts a date in YYYYMMDD format to a datetime.date object
def date_from_ymd(ymd):
    y = ymd / 10000
    m = (ymd / 100) % 100
    d = ymd % 100

    try:
        date = datetime.date(y, m, d)
    except:
        print 'Error converting ymd %d to a date' % (ymd)
        raise

    return date

# Describes a data file.  Contains a list of accounts, a list of
# buckets, the cash flow start date (as a datetime.date object), and a
# list of initial bucket balances.
class BasicInfo:
    def __init__(self, accounts, buckets, cash_flow_start, starting_bucket_balances):
        self.accounts = accounts
        self.buckets = buckets
        self.cash_flow_start = cash_flow_start
        self.starting_bucket_balances = starting_bucket_balances

# A class to represent a MoneyWell transaction.  This is a real
# transaction on an account, as opposed to a "money flow" or "bucket
# transfer" that just moves money between buckets.
class Transaction:
    def __init__(self, key, date, account, is_bucket_optional, bucket,
                 transfer_sibling, split_parent, payee, memo, amount):
        self.key = key
        self.date = date
        self.account = account
        self.is_bucket_optional = is_bucket_optional
        self.bucket = bucket
        self.transfer_sibling = transfer_sibling
        self.split_parent = split_parent
        self.payee = payee
        self.memo = memo
        self.amount = amount

    def __repr__(self):
        if self.bucket is not None:
            bucket = str(self.bucket)
        else:
            if self.is_bucket_optional:
                bucket = '(optional)'
            else:
                bucket = 'UNASSIGNED'

        xfer = ''
        if self.transfer_sibling != None:
            xfer = ' (transfer partner %d)' % (self.transfer_sibling)

        return '[%d] %s: %.2f %s (%s) [acct %s] [bkt %s]%s' % \
            (self.key, self.date.isoformat(), self.amount,
             self.payee, self.memo, self.account, bucket, xfer )

# This class represents a "money flow", which is a transfer of funds
# between buckets.  Note that this is one half of a money flow, they
# are all represented as transfers.
class MoneyFlow:
    def __init__(self, key, date, bucket, transfer_sibling, memo, amount):
        self.key = key
        self.date = date
        self.bucket = bucket
        self.transfer_sibling = transfer_sibling
        self.memo = memo
        self.amount = amount

    def __repr__(self):
        return '[%d] %s: %.2f %s [bkt %d] (xfer partner %d)' % \
            (self.key, self.date.isoformat(), self.amount, self.memo,
             self.bucket, self.transfer_sibling )

# Class to interface to a MoneyWell data file.  Provides methods for
# reading information from the data file.
class DataFile:
    def __init__(self, name):
        self.name = name
        self.is_open = 0

    def open(self):
        if self.is_open:
            self.con.close()
            self.con = None
            self.cursor = None
            self.is_open = 0

        try:
            self.con = sqlite3.connect(self.name)
        except:
            # Maybe they just specified the path to the MoneyWell data
            # "file" without the path to persistent store inside it?  Let this exception "bubble up" if this attempt fails.
            self.con = sqlite3.connect(os.path.join(self.name,'StoreContent','persistentStore'))
        
        self.cursor = self.con.cursor()
        self.is_open = 1

    def get_accounts(self):
        if not self.is_open:
            raise Exception('not open')

        self.cursor.execute('select Z_PK,ZNAME,ZINCLUDEINCASHFLOW from ZACCOUNT')

        accounts = {}
        for row in self.cursor:
            key = row[0]
            name = row[1]
            bucketed = row[2]

            accounts[key] = Account(key=key, name=name, bucketed=bucketed)
        
        return accounts

    def get_buckets(self):
        if not self.is_open:
            raise Exception('not open')

        self.cursor.execute('select Z_PK,ZNAME,ZISHIDDEN from ZBUCKET')

        buckets = {}
        for row in self.cursor:
            key = row[0]
            name = row[1]
            hidden = row[2]

            buckets[key] = Bucket(key=key, name=name, hidden=hidden)
        
        return buckets

    def get_cash_flow_start_date(self):
        if not self.is_open:
            raise Exception('not open')

        self.cursor.execute('select ZCASHFLOWSTARTDATEYMD from ZSETTINGS')
        return date_from_ymd(self.cursor.fetchone()[0])

    def get_starting_bucket_balances(self, buckets):
        if not self.is_open:
            raise Exception('not open')

        self.cursor.execute('select ZBUCKET,ZAMOUNT from ZBUCKETSTARTINGBALANCE where ZBUCKET IS NOT NULL')

        bucket_balances = []
        for row in self.cursor:
            bucket = buckets[row[0]]
            balance = row[1]

            bucket_balances.append((bucket,balance))

        return bucket_balances

    def get_basic_info(self):
        accounts = self.get_accounts()
        buckets = self.get_buckets()
        cfsd = self.get_cash_flow_start_date()
        sbb = self.get_starting_bucket_balances(buckets)

        return BasicInfo(accounts=accounts,
                         buckets=buckets,
                         cash_flow_start=cfsd,
                         starting_bucket_balances=sbb )

    def get_transactions(self):
        if not self.is_open:
            raise Exception('not open')

        self.cursor.execute('select Z_PK,ZDATEYMD,ZACCOUNT2,ZISBUCKETOPTIONAL,ZBUCKET2,ZTRANSFERSIBLING,ZSPLITPARENT,ZPAYEE,ZMEMO,ZAMOUNT from ZACTIVITY')

        transactions = {}
        for row in self.cursor:
            if row[1] == 0 and row[9] == 0:
                # Some transactions have an invalid date, and if we
                # try to convert them we get an error.  Ignore them as
                # long as the amount is also 0.
                continue
            key = row[0]
            date = date_from_ymd(row[1])
            account = row[2]
            is_bucket_optional = row[3]
            bucket = row[4]
            transfer_sibling = row[5]
            split_parent = row[6]
            payee = row[7]
            memo = row[8]
            amount = row[9]

            t = Transaction(key=key,
                            date=date,
                            account=account,
                            is_bucket_optional=is_bucket_optional,
                            bucket=bucket,
                            transfer_sibling=transfer_sibling,
                            split_parent=split_parent,
                            payee=payee,
                            memo=memo,
                            amount=amount )
            transactions[key] = t

        return transactions

    def get_money_flows(self):
        if not self.is_open:
            raise Exception('not open')

        self.cursor.execute('select Z_PK,ZDATEYMD,ZBUCKET,ZTRANSFERSIBLING,ZMEMO,ZAMOUNT from ZBUCKETTRANSFER')

        flows = {}
        for row in self.cursor:
            key = row[0]
            date = date_from_ymd(row[1])
            bucket = row[2]
            transfer_sibling = row[3]
            memo = row[4]
            amount = row[5]

            f = MoneyFlow(key=key,
                          date=date,
                          bucket=bucket,
                          transfer_sibling=transfer_sibling,
                          memo=memo,
                          amount=amount )
            flows[key] = f

        return flows
        

if __name__ == '__main__':
    df = DataFile('testdata/matt_play_copy.moneywell')
    df.open()

    info = df.get_basic_info()

    print ''
    print 'Accounts:'
    for acct in info.accounts.values():
        print acct

    print ''
    print 'Buckets:'
    for bucket in info.buckets.values():
        print bucket

    print ''
    print 'Starting bucket balances:'
    for sbb in info.starting_bucket_balances:
        print 'bucket %d: %f' % (sbb[0].key, sbb[1])

    print ''
    print 'Cash flow start date: %s' % (info.cash_flow_start.isoformat())

    txns = df.get_transactions()
    print ''
    print 'Found %d transactions' % (len(txns))

    flows = df.get_money_flows()
    print ''
    print 'Found %d money flows' % (len(flows))

