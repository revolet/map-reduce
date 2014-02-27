#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Test::Deep;
use Redis;
use MapReduce;

my $redis = Redis->new(
    encoding  => undef,
    reconnect => 60,
    server    => '127.0.0.1:6379',
);

$redis->select(9);
$redis->flushdb();

# Start up 5 mapreduce processes
my %procs;

for (1..5) {
    $procs{$_} = MapReduce::Process->new()->start();
}

my $mr = MapReduce->new(
    name => 'test1',
    
    mapper => sub {
        my ($self, $input) = @_;
        
        $input->{value} *= 2;
        
        return $input;
    },
);

# Load up some numbers to feed into our map-reduce functions
my $inputs = [ map {{ key => $_, value => $_ }} 5, 2, 3, 4, 1 ];

$mr->inputs($inputs);

my $results = $mr->all_results;
my @values  = map { $_->{value} } @$results;

cmp_deeply \@values, bag(2, 4, 6, 8, 10), 'Got all results from map-reduce operation';

ok !defined $mr->next_result(), 'No more results, so we get undefined';
ok !defined $mr->next_result(), 'No more results, so we get undefined';
ok !defined $mr->next_result(), 'No more results, so we get undefined';

done_testing;

