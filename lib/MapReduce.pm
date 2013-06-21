package MapReduce;
use Moo;
use Storable qw(nfreeze thaw);
use B::Deparse;
use Time::HiRes qw(time);
use Carp qw(croak);
use List::MoreUtils qw(all);
use MapReduce::Mapper;
use MapReduce::Reducer;

# Do not change these. Rather, to enable logging,
# change the $LOGGING value to one of these variables.
our $DEBUG = 2;
our $INFO  = 1;
our $NONE  = 0;

# Enable / disable logging.
our $LOGGING = $ENV{MAPREDUCE_LOGGING} // $NONE;

sub debug { shift->log('DEBUG', @_) if $LOGGING >= $DEBUG }
sub info  { shift->log('INFO',  @_) if $LOGGING >= $INFO  }

sub log {
    my ($class, $level, $format, @args) = @_;

    $format //= '';
    
    printf STDERR $level.': '.$format."\n", map { defined $_ ? $_ : 'undef' } @args;
}

has [ qw( name mapper reducer ) ] => (
    is       => 'ro',
    required => 1,
);

has id => (
    is => 'lazy',
);

with 'MapReduce::Role::Redis';

sub _build_id {
    my ($self) = @_;
    
    return $self->name . '-' . time . '-' . rand;
}

sub BUILD {
    my ($self) = @_;
    
    my $deparse = B::Deparse->new();
    my $mapper  = $deparse->coderef2text( $self->mapper  );
    my $reducer = $deparse->coderef2text( $self->reducer );
    my $redis   = $self->redis;
    
    MapReduce->debug( "Mapper is '%s'",  $mapper );
    MapReduce->debug( "Reducer is '%s'", $reducer );
    
    $redis->hset( mapper  => ( $self->id => $mapper  ) );
    $redis->hset( reducer => ( $self->id => $reducer ) );
}

sub input {
    my $self = shift;
    
    croak 'Please surround your input operation with input_start() and input_done()'
        if !$self->redis->get($self->id.'-inputting');
    
    # Support a hash or hash ref for the input
    my $input = @_ == 1 ? shift : {@_};
    
    my $redis = $self->redis;
    
    $redis->lpush( $self->id.'-input', nfreeze($input) );
    
    MapReduce->debug( "Pushed input '%s' to %s->input.", $input->{key}, $self->id );
    
    return $self;
}

sub input_start {
    my ($self) = @_;
    
    $self->redis->set( $self->id.'-inputting', 1 );
}

sub input_done {
    my ($self) = @_;
    
    $self->redis->del( $self->id.'-inputting' );
}

sub done {
    my ($self) = @_;
    
    my $redis = $self->redis;
    my $id    = $self->id;
    
    $redis->multi;
    
    $redis->llen( $id.'-input'     );
    $redis->llen( $id.'-mapped'    );
    $redis->llen( $id.'-reduced'   );
    $redis->get(  $id.'-mapping'   );
    $redis->get(  $id.'-reducing'  );
    $redis->get(  $id.'-inputting' );
    
    my $values = $redis->exec;
    
    my $done = all { !$_ } @$values;
    
    MapReduce->debug( "Input queue:   %s", $values->[0] );
    MapReduce->debug( "Mapped queue:  %s", $values->[1] );
    MapReduce->debug( "Reduced queue: %s", $values->[2] );
    MapReduce->debug( "Mapping?:      %s", $values->[3] );
    MapReduce->debug( "Reducing?:     %s", $values->[4] );
    MapReduce->debug( "Inputting?:    %s", $values->[5] );
    
    # TODO: This needs to be done in the Mapper and Reducer classes
    if ($done) {
        $redis->hdel( mapper  => $self->id );
        $redis->hdel( reducer => $self->id );
    }
    
    return $done;
}

sub next_result {
    my ($self) = @_;
    
    my $redis = $self->redis;
    
    while (1) {
        return undef if $self->done;

        my $reduced = $redis->brpop( $self->id.'-reduced', 1);
        
        next if !defined $reduced;
        next if !defined $reduced->[1];
        
        my $value = thaw($reduced->[1]);
        
        croak 'Reduced result is undefined?'
            if !defined $value;
        
        return $value;
    }
}

sub all_results {
    my ($self) = @_;
    
    my @results;
    
    while (!$self->done) {
        my $result = $self->next_result;
        next if !defined $result;
        push @results, $result;
    }
    
    return \@results;
}

1;

