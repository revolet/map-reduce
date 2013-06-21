## map-reduce

A simple MapReduce platform using Perl 5 and Redis

### Example

This example will calculate the number of characters in each word of the sentence and
then drop any that have less than 3 characters.

```perl
#!/usr/bin/env perl
use strict;
use warnings;
use Redis::hiredis;
use MapReduce;

# This is necessary at the moment until we can improve redis integration
my $redis = Redis::hiredis->new();

$redis->connect('127.0.0.1', 6379);
$redis->select(9);
$redis->flushdb();

# We have to save references to the mappers and reducers to avoid
# them getting garbage collected immediately.
my @workers;

# Fork one mapper (you can use several)
for (1..1) {
    push @workers, MapReduce::Mapper->new(
        daemon => 1,
    );
}

# Fork one reducer (currently only one is useful)
push @workers, MapReduce::Reducer->new(
    daemon => 1,
);

my $mr = MapReduce->new(
    name    => 'count-chars',
    mapper  => \&mapper,
    reducer => \&reducer,
);

my $text = 'MapReduce is a programming model for processing large data sets in parallel.';

my @words = split qr{\s+}, $text;

for my $index (0 .. $#words) {
    $mr->input({
        key  => $index,
        word => $words[$index],
    });
}

my $results = $mr->all_results;

for my $result (@$results) {
    printf STDERR "Character count for word '%s': %s\n",
        $result->{word}, $result->{count};
}

sub mapper {
    my ($mapper, $input) = @_;
    
    $input->{count} = length $input->{word};
    
    return $input;
}

sub reducer {
    my ($reducer, $inputs) = @_;
    
    # Keep only inputs with a count > 3
    return [ grep { $_->{count} > 3 } @$inputs ];
}
```

