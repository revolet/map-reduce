#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Test::Deep;
use Redis::hiredis;
use MapReduce qw(pmap);

my $redis = Redis::hiredis->new(utf8 => 0);

$redis->connect('127.0.0.1', 6379);
$redis->select(9);
$redis->flushdb();

# Load up some numbers to feed into our map-reduce functions
my $inputs = [ map {{ key => $_, value => $_ }} 5, 2, 3, 4, 1 ];

my $values = pmap { $_*2 } [1 .. 5];

cmp_deeply $values, bag(2, 4, 6, 8, 10), 'Got all results from map-reduce operation';

done_testing;

