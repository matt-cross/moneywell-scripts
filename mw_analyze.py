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
## Transactions with buckets assigned in an account that is excluded
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

    # A method to get the date, useful for sorting transactions by date.
    def get_date(self):
        return self.date

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



# A simple date range class
class DateRange:
    def __init__(self, datestart, dateend):
        self.datestart = datestart
        self.dateend = dateend

    def includes_date(self, date):
        return self.datestart <= date and date <= self.dateend

    def __repr__(self):
        return '%s to %s' % (self.datestart.isoformat(), self.dateend.isoformat())


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

        # Create a set containing the keys of all split transactions.
        # The only way to find this out is to look for transactions
        # that have a "split_parent".
        split_children = [txn for txn in transactions.values() if txn.split_parent]
        self.splits = set(map(lambda txn: txn.split_parent, split_children))

        # Some accounts are bucketed for some of the history and not
        # for other parts.  It's OK for accounts to transition between
        # bucketed and unbucketed when their balances are 0.  This is
        # a dictionary of accounts that are like this: the key is the
        # account's key, and the value is a list of DateRange's where
        # the account was bucketed.
        self.semi_bucketed_accounts = {}

    def add_account_bucketed_daterange(self, account, date_range):
        if account in self.semi_bucketed_accounts:
            self.semi_bucketed_accounts[account].append(date_range)
        else:
            self.semi_bucketed_accounts[account] = [date_range]

    def print_sometimes_bucketed_accounts(self):
        if len(self.semi_bucketed_accounts.keys()):
            print 'List of accounts that are sometimes bucketed:'
            for account in self.semi_bucketed_accounts.keys():
                print '  account %2d (%25s) is bucketed in date ranges: %s' % (account, self.accounts[account].name, self.semi_bucketed_accounts[account])

    # Check if a transaction is a split.  'transaction' may be either the
    # primary key of a transaction or a Transaction object.
    def is_txn_split(self, transaction):
        if isinstance(transaction, Transaction):
            transaction = transaction.key
        return transaction in self.splits

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
    def is_account_bucketed(self, account, date):
        if account in self.semi_bucketed_accounts:
            # Semi-bucketed accounts override the information in the data file.
            for date_range in self.semi_bucketed_accounts[account]:
                if date_range.includes_date(date):
                    return True
            return False
        else:
            return self.accounts[account].bucketed

    # Return a list of the primary keys of all permanently bucketed accounts
    def permanently_bucketed_accounts(self):
        return [account for account in self.accounts.keys() if self.accounts[account].bucketed and account not in self.semi_bucketed_accounts]

    # Return a list of the primary keys of all permanently unbucketed accounts
    def permanently_unbucketed_accounts(self):
        return [account for account in self.accounts.keys() if not self.accounts[account].bucketed and account not in self.semi_bucketed_accounts]

    # Return a list of the primary keys of all accounts that are sometimes bucketed
    def sometimes_bucketed_accounts(self):
        return self.semi_bucketed_accounts.keys()

    # Return a list of the primary keys of accounts that are bucketed on the specified date
    def bucketed_accounts(self, date):
        return [account for account in self.accounts.keys() if self.is_account_bucketed(account, date)]

    # Return a list of the primary keys of accounts that are unbucketed on the specified date
    def unbucketed_accounts(self, date):
        return [account for account in self.accounts.keys() if not self.is_account_bucketed(account, date)]

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
        return self.total_account_balance(self.bucketed_accounts(date), date)

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
        my_txns = txns_between_dates(txns_in_bucket(self.transactions, bucket), self.cash_flow_start + datetime.timedelta(days=1), date)
        txn_balance = txn_amount_sum(my_txns)

        # Finally, calculate the sum of all money flows that affect this bucket:
        my_flows = flows_between_dates(flows_in_bucket(self.money_flows, bucket), self.cash_flow_start + datetime.timedelta(days=1), date)
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
            accounts_to_include = self.bucketed_accounts(self.cash_flow_start)

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

        return account_balance_total - bucket_balance_total

    # Check the sum of bucket balances versus the sum of account
    # balances as of the specified date (or now if date is not
    # specified).  Print out a report of the results.
    def check_bucket_balances(self, date = datetime.date.max):
        account_sum = self.total_bucketed_account_balance(date)
        bucket_sum = self.total_bucket_balance(date)

        if date == datetime.date.max:
            datestr = ''
        else:
            datestr = ' [as of %s]' % (date.isoformat())

        if account_sum == bucket_sum:
            print 'Bucket vs. account check: good (%.2f == %.2f)%s' % (account_sum, bucket_sum, datestr)
            return True
        else:
            if account_sum > bucket_sum:
                print '  *** ERROR: accounts exceed bucket balance by %.2f (accounts: %.2f, buckets %.2f)%s' % \
                    (account_sum - bucket_sum, account_sum, bucket_sum, datestr)
            else:
                print '  *** ERROR: bucket balance exceeds accounts by %.2f (accounts: %.2f, buckets %.2f)%s' % \
                    (bucket_sum - account_sum, account_sum, bucket_sum, datestr)
            return False

    # Return true if this transaction is a transfer and the other side
    # of the transaction is going into (or out of) a bucketed account.
    def is_txn_xfer_sibling_bucketed(self, txn):
        if txn.transfer_sibling not in self.transactions:
            return False # Not a transfer or missing sibling

        sibling = self.transactions[txn.transfer_sibling]
        return self.is_account_bucketed(sibling.account, txn.date)


    # Given a transaction return its transfer sibling.
    def get_xfer_sibling(self, txn):
        if txn.transfer_sibling not in self.transactions:
            return None

        return self.transactions[txn.transfer_sibling]

    # The following methods are designed to catch entry errors that
    # would lead to the sum of bucket balances not equaling account
    # balances.  They all return a sum of the errors they found.  The
    # sign of the error will be as if the expected expression is
    # "account_balances - bucket_balances = error".  So if there is
    # one credit (positive) transaction in a bucketed account that
    # does not have a bucket assigned, the error will be positive.
    # However, if that same transaction were bucketed but in an
    # unbucketed account, the error would be negative.


    # Print out a list of all transactions in bucketed accounts that
    # don't have buckets assigned.
    def check_for_unbucketed_txns_in_bucketed_accounts(self):
        error_sum = 0.0

        for account in set(self.permanently_bucketed_accounts()) | set(self.sometimes_bucketed_accounts()):
            # Start with a list of all transactions in this account starting on the cash flow start date
            txns = txns_in_account(self.transactions, account)
            txns = txns_between_dates(txns, self.cash_flow_start + datetime.timedelta(days=1), datetime.date.max)

            if account in self.sometimes_bucketed_accounts():
                # Only include transactions during the time(s) the account was bucketed
                txns = [txn for txn in txns if self.is_account_bucketed(account, txn.date)]

            # Exclude any split parent transactions
            txns = [txn for txn in txns if not self.is_txn_split(txn)]

            # Exclude any transfers
            txns = [txn for txn in txns if not txn.transfer_sibling]

            # Filter out all transactions that have a bucket assigned
            txns = [txn for txn in txns if txn.bucket == None]

            # Filter out transactions with an amount of 0 (since these won't impact balances anyway)
            txns = [txn for txn in txns if txn.amount]

            # Sort them by date
            txns.sort(key = Transaction.get_date)

            if txns:
                error_this_account = txn_amount_sum(txns)
                error_sum += error_this_account
                print '  ***'
                print '  *** Bucketed account %d (%s) has %d transaction(s) without buckets totalling %.2f:' % \
                    (account, self.accounts[account].name, len(txns), error_this_account)
                for txn in txns:
                    print '  *** %s' % (txn)
                print '  ***'

        if error_sum:
            print '  *** Sum of unbucketed transactions in bucketed accounts: %.2f' % (error_sum)
        else:
            print '  No issues found.'

        return error_sum

    # Print out a list of all transactions in unbucketed accounts that
    # have buckets assigned.
    def check_for_bucketed_txns_in_unbucketed_accounts(self):
        error_sum = 0.0

        for account in set(self.permanently_unbucketed_accounts()) | set(self.sometimes_bucketed_accounts()):

            # Start with a list of all transactions in this account starting on the cash flow start date
            txns = txns_in_account(self.transactions, account)
            txns = txns_between_dates(txns, self.cash_flow_start + datetime.timedelta(days=1), datetime.date.max)

            if account in self.sometimes_bucketed_accounts():
                # Only include transactions during the time(s) the account was unbucketed
                txns = [txn for txn in txns if not self.is_account_bucketed(account, txn.date)]

            # Exclude any split parent transactions
            txns = [txn for txn in txns if not self.is_txn_split(txn)]

            # Exclude any transfers
            txns = [txn for txn in txns if not txn.transfer_sibling]

            # Filter out all transactions that do not have a bucket assigned
            txns = [txn for txn in txns if txn.bucket != None]

            # Filter out transactions with an amount of 0 (since these won't impact balances anyway)
            txns = [txn for txn in txns if txn.amount]

            # Sort them by date
            txns.sort(key = Transaction.get_date)

            if txns:
                error_this_account = txn_amount_sum(txns)
                error_sum += error_this_account
                print '  ***'
                print '  *** Unbucketed account %d (%s) has %d transaction(s) with buckets totalling %.2f:' % \
                    (account, self.accounts[account].name, len(txns), error_this_account)
                for txn in txns:
                    print '  *** %s' % (txn)
                print '  ***'

        # See the above note about the sign of errors reported by this method
        error_sum = -error_sum

        if error_sum:
            print '  *** Sum of bucketed transactions in unbucketed accounts: %.2f' % (error_sum)
        else:
            print '  No issues found.'

        return error_sum

    # Check that all splits have split children that add up to the split parent.
    def check_splits(self):
        error_sum = 0.0
        error_sum_bucketed = 0.0
        error_count = 0

        for txn_key in self.splits:
            parent = self.transactions[txn_key]
            children = [txn for txn in self.transactions.values() if txn.split_parent == txn_key]

            error = parent.amount - txn_amount_sum(children)

            if abs(error) >= 0.01:
                print '  ***'
                print '  *** Incomplete split transation (unsplit amount is %.2f):' % (error)
                print '  ***   Parent:'
                print '  ***     %s' % (parent)
                print '  ***'
                print '  ***   Children:'
                for child in children:
                    print '  ***     %s' % (child)
                print '  ***'
                error_count += 1
                error_sum += error
                if self.is_account_bucketed(parent.account, parent.date) and parent.date > self.cash_flow_start:
                    error_sum_bucketed += error

        if error_count:
            print '  *** Found %d split transaction(s) with errors' % (error_count)
        if error_sum:
            print '  *** Total of errors: %.2f' % (error_sum)
        if error_sum_bucketed:
            print '  *** Total of errors in bucketed accounts: %.2f' % (error_sum_bucketed)

        if error_count == 0:
            print '  No issues found.'

        # We return the error only as it applies to bucketed accounts.
        # Any unsplit portion in an unbucketed account does not affect
        # bucket/account mismatches.

        return error_sum_bucketed

    # Check that transfers between bucketed accounts have no buckets
    # assigned, and that transfers between bucketed and unbucketed
    # accounts have buckets on the bucketed side.
    def check_bucketed_account_transfers(self):
        error_sum = 0.0

        for account in set(self.permanently_bucketed_accounts()) | set(self.sometimes_bucketed_accounts()):
            # Get a list of all transfers in this account (after the cash flow start date)
            txns = txns_in_account(self.transactions, account)
            txns = txns_between_dates(txns, self.cash_flow_start + datetime.timedelta(days=1), datetime.date.max)

            if account in self.sometimes_bucketed_accounts():
                # Only include transactions during the time(s) the account was bucketed
                txns = [txn for txn in txns if self.is_account_bucketed(account, txn.date)]
                
            xfers = [txn for txn in txns if txn.transfer_sibling]

            # Split them into the ones going to bucketed and unbucketed accounts
            xfers_to_bucketed = [txn for txn in xfers if self.is_txn_xfer_sibling_bucketed(txn)]
            xfers_to_unbucketed = [txn for txn in xfers if not self.is_txn_xfer_sibling_bucketed(txn)]

            # Get a list of all transfers to bucketed accounts that have buckets assigned (they shouldn't):
            xfers_to_bucketed = [txn for txn in xfers_to_bucketed if txn.bucket]
            xfers_to_bucketed.sort(key = Transaction.get_date)

            # Get a list of all transfers to unbucketed accounts that don't have buckets assigned (they should):
            xfers_to_unbucketed = [txn for txn in xfers_to_unbucketed if not txn.bucket]
            xfers_to_unbucketed.sort(key = Transaction.get_date)

            if xfers_to_bucketed:
                error_this_time = txn_amount_sum(xfers_to_bucketed)
                error_sum += error_this_time
                print '  ***'
                print '  *** Bucketed account %d (%s) has %d transfer(s) to another bucketed account with buckets assigned totalling %.2f:' % \
                    (account, self.accounts[account].name, len(xfers_to_bucketed), error_this_time)
                for txn in xfers_to_bucketed:
                    print '  *** %s' % (txn)
                    sibling = self.get_xfer_sibling(txn)
                    if sibling:
                        print '  ***** ^-> %s' % (sibling)
                        print '  *****'
                print '  ***'

            if xfers_to_unbucketed:
                error_this_time = txn_amount_sum(xfers_to_unbucketed)
                error_sum += error_this_time
                print '  ***'
                print '  *** Bucketed account %d (%s) has %d transfer(s) to unbucketed accounts without buckets assigned totalling %.2f:' % \
                    (account, self.accounts[account].name, len(xfers_to_unbucketed), error_this_time)
                for txn in xfers_to_unbucketed:
                    print '  *** %s' % (txn)
                    sibling = self.get_xfer_sibling(txn)
                    if sibling:
                        print '  ***** ^-> %s' % (sibling)
                        print '  *****'
                print '  ***'

        if error_sum:
            print '  *** Sum of incorrect bucketed transfers in bucketed accounts: %.2f' % (error_sum)
        else:
            print '  No issues found.'

        return error_sum

    # Check that transfers in unbucketed accounts have no buckets
    # assigned - this is true whether the other side is a bucketed or
    # unbucketed account.
    def check_unbucketed_account_transfers(self):
        error_sum = 0.0

        for account in set(self.permanently_unbucketed_accounts()) | set(self.sometimes_bucketed_accounts()):

            # Get a list of all transfers in this account (after the cash flow start date)
            txns = txns_in_account(self.transactions, account)
            txns = txns_between_dates(txns, self.cash_flow_start + datetime.timedelta(days=1), datetime.date.max)

            if account in self.sometimes_bucketed_accounts():
                # Only include transactions during the time(s) the account was unbucketed
                txns = [txn for txn in txns if not self.is_account_bucketed(account, txn.date)]

            xfers = [txn for txn in txns if txn.transfer_sibling]

            # Get a list of all transfers that have buckets assigned (they shouldn't):
            xfers = [txn for txn in xfers if txn.bucket]
            xfers.sort(key = Transaction.get_date)

            if xfers:
                error_this_time = txn_amount_sum(xfers)
                error_sum += error_this_time
                print '  ***'
                print '  *** Unbucketed account %d (%s) has %d transfer(s) with buckets assigned totalling %.2f:' % \
                    (account, self.accounts[account].name, len(xfers), error_this_time)
                for txn in xfers:
                    print '  *** %s' % (txn)
                    sibling = self.get_xfer_sibling(txn)
                    if sibling:
                        print '  ***** ^-> %s' % (sibling)
                        print '  *****'
                print '  ***'

        # See the note above about the sign of the error reported by these methods.
        error_sum = -error_sum

        if error_sum:
            print '  *** Sum of bucketed transfers in unbucketed accounts: %.2f' % (error_sum)
        else:
            print '  No issues found.'

        return error_sum

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

# Setup for our specific moneywell file:
def cross_setup(info):
    # Some of our accounts were only bucketed for some of the history range:
    info.add_account_bucketed_daterange(info.account_id_from_name('DCU Visa Gold'),
                                        DateRange(datetime.date(2012,9,29), datetime.date(2013,6,12)) )
    info.add_account_bucketed_daterange(info.account_id_from_name("The Children's Place CC"),
                                        DateRange(datetime.date(2012,9,29), datetime.date(2013,6,1)) )

if __name__ == '__main__':
    info = read_in_basic_info('testdata/matt_play_copy.moneywell')

    verbose = 1

    if verbose:
        print ''
        print 'Accounts:'
        max_account_name_length = max([len(account.name) for account in info.accounts.values()])
        for acct in info.accounts.values():
            if acct.bucketed:
                bucketed_str = ' (bucketed)'
            else:
                bucketed_str = ''
            print '  %2d: %-*s %10.2f%s' % (acct.key, max_account_name_length, acct.name, info.account_balance(acct.key), bucketed_str)

        print ''
        print 'Buckets:'
        max_bucket_name_length = max([len(bucket.name) for bucket in info.buckets.values()])
        buckets = info.buckets.values()
        buckets.sort(key = lambda bucket: (bucket.hidden, bucket.key))
        for bucket in buckets:
            if bucket.hidden:
                bucket_name = '(%s)' % (bucket.name)
            else:
                bucket_name = bucket.name
            print '  %2d: %-*s %10.2f' % (bucket.key, max_bucket_name_length+2, bucket_name, info.bucket_balance(bucket.key))

        print ''
        print 'Starting bucket balances:'
        for bucket in info.starting_bucket_balances.keys():
            if info.starting_bucket_balances[bucket]:
                if info.buckets[bucket].hidden:
                    bucket_name = '(%s)' % (info.buckets[bucket].name)
                else:
                    bucket_name = info.buckets[bucket].name
                print '  %2d:  %-*s: %10.2f' % (bucket, max_bucket_name_length, bucket_name, info.starting_bucket_balances[bucket])

    print ''
    print 'Cash flow start date: %s' % (info.cash_flow_start.isoformat())

    print ''
    print 'Found %d transactions' % (len(info.transactions))

    print ''
    print 'Found %d money flows' % (len(info.money_flows))

    cross_setup(info)

    print ''
    info.print_sometimes_bucketed_accounts()

    print ''
    print 'Checking bucket balances against bucketed account balances:'
    info.check_bucket_balances()

    error_sum = 0.0

    print ''
    print 'Checking cash flow start:'
    error_sum += info.check_cash_flow_start()

    print ''
    print 'Checking for bucketed transactions in unbucketed accounts:'
    error_sum += info.check_for_bucketed_txns_in_unbucketed_accounts()

    print ''
    print 'Checking for unbucketed transactions in bucketed accounts:'
    error_sum += info.check_for_unbucketed_txns_in_bucketed_accounts()

    print ''
    print 'Checking split transactions for consistency:'
    error_sum += info.check_splits();

    print ''
    print 'Checking transfers in bucketed accounts:'
    error_sum += info.check_bucketed_account_transfers()

    print ''
    print 'Checking transfers in unbucketed accounts:'
    error_sum += info.check_unbucketed_account_transfers()

    print ''
    print 'Done.'

    print ''
    print '  *** Sum of discovered errors: %.2f' % (error_sum)
