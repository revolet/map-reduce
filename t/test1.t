#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Test::Deep;
use Redis::hiredis;
use MR;

my $redis = Redis::hiredis->new();

$redis->connect('127.0.0.1', 6379);
$redis->select(9);
$redis->flushdb();

my $mr = MR->new(
    name => 'test1',
    
    mapper => sub {
        my ($self, $input) = @_;
        
        $input->{value} *= 2;
        
        return $input;
    },
    
    reducer => sub {
        my ($self, $nums) = @_;
        
        my %seen;
        
        return [ grep { !$seen{ $_->{value} }++ } @$nums ];
    }
);

# Load up some numbers to feed into our map-reduce functions
my $inputs = [ map {{ key => $_, value => $_ }} 5, 2, 3, 4, 1 ];

$mr->input($_) for @$inputs;

# Start up 5 mapper processes
my %mappers;

for (1..5) {
    $mappers{$_} = MR::Mapper->new(daemon => 1);
}

# Start up 1 reducer process
my $reducer = MR::Reducer->new(daemon => 1);

my $results = $mr->all_results;
my @values  = map { $_->{value} } @$results;

cmp_deeply \@values, bag(2, 4, 6, 8, 10), 'Got all results from map-reduce operation';

ok !defined $mr->next_result(), 'No more results, so we get undefined';
ok !defined $mr->next_result(), 'No more results, so we get undefined';
ok !defined $mr->next_result(), 'No more results, so we get undefined';

done_testing;

