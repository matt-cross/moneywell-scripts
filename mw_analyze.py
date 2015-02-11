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

#
# Utility functions for dealing with lists of transactions and flows.
# Transactions are stored by default in BasicInfo as a dictionary so
# that we can find them by transaction ID, but these filters all
# return a list of transactions.  They can take either a dictionary or
# list of transactions.
#

# Filter out the split children transactions - they are not useful for computing the balance of an account.
def proper_txns(txns):
    if isinstance(txns, dict):
        txns = txns.values()
    return [txn for txn in txns if txn.split_parent is None]

# Return a list of transactions that are for the specified account
def txns_in_account(txns, account):
    if isinstance(txns, dict):
        txns = txns.values()
    return [txn for txn in txns if txn.account == account]

# Return a list of transactions that are assigned to the specified bucket
def txns_in_bucket(txns, bucket):
    if isinstance(txns, dict):
        txns = txns.values()
    return [txn for txn in txns if txn.bucket == bucket]

# Returns transactions that are on the start date, end date, and every date in between
def txns_between_dates(txns, datestart, dateend):
    if isinstance(txns, dict):
        txns = txns.values()
    return [txn for txn in txns if txn.date >= datestart and txn.date <= dateend]

# Returns a list of transactions that are on or before the specified date
def txns_at_or_before_date(txns, date):
    return txns_between_dates(txns, datetime.date.min, date)

# Returns a sum of the amount of all transactions in the list/dictionary:
def txn_amount_sum(txns):
    if isinstance(txns, dict):
        txns = txns.values()
    return round(sum(map(lambda txn: txn.amount, txns)), 2)

# Returns a list of money flows that affect the specified bucket
def flows_in_bucket(flows, bucket):
    if isinstance(flows, dict):
        flows = flows.values()
    return [flow for flow in flows if flow.bucket == bucket]

# Returns a list of flows that are on the start date, end date, and every date in between
def flows_between_dates(flows, datestart, dateend):
    if isinstance(flows, dict):
        flows = flows.values()
    return [flow for flow in flows if flow.date >= datestart and flow.date <= dateend]

# Returns a sum of the amount of all money flows in the list/dictionary:
def flow_amount_sum(flows):
    if isinstance(flows, dict):
        flows = flows.values()
    return round(sum(map(lambda flow: flow.amount, flows)), 2)



# Describes a data file.  Contains a list of accounts, a list of
# buckets, the cash flow start date (as a datetime.date object), a
# list of initial bucket balances, the list of transactions and the
# list of money flows (aka bucket transfers).
class BasicInfo:
    def __init__(self, accounts, buckets, cash_flow_start, starting_bucket_balances, transactions, money_flows):
        self.accounts = accounts
        self.buckets = buckets
        self.cash_flow_start = cash_flow_start
        self.starting_bucket_balances = starting_bucket_balances
        self.transactions = transactions
        self.money_flows = money_flows

    # Returns the ID of an account given its name.  Returns None if
    # account with that name was not found.
    def account_id_from_name(self, account_name):
        for account in self.accounts.values():
            if account.name == account_name:
                return account.key
        return None

    # Returns true if the account is bucketed (IE included in cash
    # flow comparison), or false if the account is not.  'account'
    # should be the account primary key (small integer account
    # number).
    def is_account_bucketed(self, account):
        return self.accounts[account].bucketed

    # Return a list of the primary keys of all bucketed accounts
    def bucketed_accounts(self):
        return [account for account in self.accounts.keys() if self.is_account_bucketed(account)]

    # Returns the balance of the account at the end of the specified
    # date (or as of all transactions in the register if date was not
    # specified).  'account' is the account's primary key (a small
    # integer), and date must be a datetime.date object.
    def account_balance(self, account, date = None):
        txns = txns_in_account(proper_txns(self.transactions), account)
        if date:
            txns = txns_at_or_before_date(txns, date)

        return txn_amount_sum(txns)

    # Returns a sum of the balances of all specified accounts as of
    # the specified date (or the current balance if date is not
    # specified).  'accounts' must be a list of account primary keys
    # (small integers).  'date' must be a datetime.date object.
    def total_account_balance(self, accounts, date = datetime.date.max):
        balances = map(lambda account: self.account_balance(account, date), accounts)

        return round(sum(balances), 2)

    # Returns a sum of the balances of all bucketed accounts as of the
    # specified date (or the current balance if date is not
    # specified).  'date' must be a datetime.date object.
    def total_bucketed_account_balance(self, date = datetime.date.max):
        return self.total_account_balance(self.bucketed_accounts(), date)

    # Returns the balance of the bucket at the end of the specified
    # date (or as of all transactions and money flows in the data file
    # if date was not specified).  'bucket' is the buckets's primary
    # key (a small integer), and date must be a datetime.date object.
    def bucket_balance(self, bucket, date = datetime.date.max):
        # The balance in a bucket includes its starting balance as of
        # the cash flow start date, transactions with buckets
        # assigned, and explicit "money flows" which are transfers
        # between buckets.  Start by getting the starting balance.
        if bucket in self.starting_bucket_balances:
            starting_balance = self.starting_bucket_balances[bucket]
        else:
            starting_balance = 0

        # Next, calculate the sum of all transactions that are
        # assigned to this bucket.  Note that we can only consider
        # transactions that are on or after the cash flow start date.
        my_txns = txns_between_dates(txns_in_bucket(self.transactions, bucket), self.cash_flow_start, date)
        txn_balance = txn_amount_sum(my_txns)

        # Finally, calculate the sum of all money flows that affect this bucket:
        my_flows = flows_between_dates(flows_in_bucket(self.money_flows, bucket), self.cash_flow_start, date)
        flow_balance = flow_amount_sum(my_flows)

        return round(starting_balance + txn_balance + flow_balance, 2);

    def total_bucket_balance(self, date = datetime.date.max):
        balances = map(lambda bucket: self.bucket_balance(bucket, date), self.buckets.keys())

        return round(sum(balances), 2)

    # Check that the sum of the bucket starting balances matches the
    # balance of the listed accounts on the cash flow start date.  If
    # no accounts are specified, this will use the accounts that are
    # 'bucketed'.
    def check_cash_flow_start(self, accounts_to_include = None):
        if accounts_to_include == None:
            accounts_to_include = self.bucketed_accounts()

        bucket_balance_total = round(sum(self.starting_bucket_balances.values()), 2)

        account_balances = map(lambda account: (account, self.account_balance(account, self.cash_flow_start)), accounts_to_include)

        account_balance_total = round(sum(map(lambda ab: ab[1], account_balances)), 2)

        if account_balance_total == bucket_balance_total:
            print 'Cash flow start check: good (%.2f == %.2f)' % (bucket_balance_total, account_balance_total)
        else:
            print '  ***'
            if account_balance_total > bucket_balance_total:
                print '  *** ERROR: accounts exceed bucket balance at cash flow start date by %.2f' % (account_balance_total - bucket_balance_total)
            else:
                print '  *** ERROR: buckets exceed account balance at cash flow start date by %.2f' % (bucket_balance_total - account_balance_total)
            print '  ***'
            print '  *** Cash flow start date: %s' % (self.cash_flow_start.isoformat())
            print '  *** Sum of bucket balances at cash flow start: %.2f' % (bucket_balance_total)
            print '  *** Sum of account balances at cash flow start: %.2f' % (account_balance_total)
            print '  ***'
            print '  *** Account balances on cash flow start date:'
            for account_balance in account_balances:
                account = account_balance[0]
                balance = account_balance[1]
                print '  ***   %d: %.2f (%s)' % (account, balance, self.accounts[account].name)

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

        bucket_balances = {}
        for row in self.cursor:
            bucket = row[0]
            balance = row[1]

            bucket_balances[bucket] = balance

        return bucket_balances

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

    def get_basic_info(self):
        accounts = self.get_accounts()
        buckets = self.get_buckets()
        cfsd = self.get_cash_flow_start_date()
        sbb = self.get_starting_bucket_balances(buckets)
        transactions = self.get_transactions()
        flows = self.get_money_flows()

        return BasicInfo(accounts = accounts,
                         buckets = buckets,
                         cash_flow_start = cfsd,
                         starting_bucket_balances = sbb,
                         transactions = transactions,
                         money_flows = flows)

def read_in_basic_info(filename):
    df = DataFile(filename)
    df.open()
    return df.get_basic_info()

if __name__ == '__main__':
    info = read_in_basic_info('testdata/matt_play_copy.moneywell')

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
    for bucket in info.starting_bucket_balances.keys():
        print 'bucket %d: %.2f' % (bucket, info.starting_bucket_balances[bucket])

    print ''
    print 'Cash flow start date: %s' % (info.cash_flow_start.isoformat())

    print ''
    print 'Found %d transactions' % (len(info.transactions))

    print ''
    print 'Found %d money flows' % (len(info.money_flows))

    # In our moneywell data file, it turns out that we have two
    # accounts as bucketed now, but on the "cash flow start date",
    # only one account was considered bucketed at that time.  The data
    # file does not contain this information.
    cash_flow_start_account_names = ['Main Checking']
    cash_flow_start_accounts = map(info.account_id_from_name, cash_flow_start_account_names)

    print ''
    info.check_cash_flow_start(cash_flow_start_accounts)

