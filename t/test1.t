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
        my ($input) = @_;
        
        $input->{value} *= 2;
        
        return $input;
    },
    
    reducer => sub {
        my ($nums) = @_;
        
        my %seen;
        
        return [ grep { !$seen{ $_->{value} }++ } @$nums ];
    }
);

# Load up some numbers to feed into our map-reduce functions
my $inputs = [ map {{ key => $_, value => $_ }} 5, 2, 3, 4, 1 ];

$mr->input($_) for @$inputs;

# Puts the mapper and reducer subs in redis
$mr->run();

# Start up 5 mapper processes
my %mappers;

for (1..5) {
    $mappers{$_} = MR::Mapper->new();
    $mappers{$_}->run();
}

# Start up 1 reducer process
my $reducer = MR::Reducer->new();
$reducer->run();

# Fetch result values as they become available
my @values;

while (!$mr->done) {
    my $value = $mr->next_result();
    
    next if !defined $value;
    
    push @values, $value->{value};
}

cmp_deeply \@values, bag(2, 4, 6, 8, 10), 'Got all results from map-reduce operation';

ok !defined $mr->next_result(), 'No more results, so we get undefined';
ok !defined $mr->next_result(), 'No more results, so we get undefined';
ok !defined $mr->next_result(), 'No more results, so we get undefined';

done_testing;

