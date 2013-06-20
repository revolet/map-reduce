package MapReduce;
use Moo;
use Storable qw(nfreeze thaw);
use B::Deparse;
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

with 'MapReduce::Role::Redis';

sub BUILD {
    my ($self) = @_;
    
    my $deparse = B::Deparse->new();
    my $mapper  = $deparse->coderef2text( $self->mapper  );
    my $reducer = $deparse->coderef2text( $self->reducer );
    my $redis   = $self->redis;
    
    MapReduce->debug( "Mapper is '%s'",  $mapper );
    MapReduce->debug( "Reducer is '%s'", $reducer );
    
    $redis->hset( mapper  => ( $self->name => $mapper  ) );
    $redis->hset( reducer => ( $self->name => $reducer ) );
}

sub input {
    my $self = shift;
    
    # Support a hash or hash ref for the input
    my $input = @_ == 1 ? shift : {@_};
    
    my $redis = $self->redis;
    
    $redis->lpush( $self->name.'-input', nfreeze($input) );
    
    MapReduce->debug( "Pushed input '%s' to %s->input.", $input->{key}, $self->name );
    
    return $self;
}

sub done {
    my ($self) = @_;
    
    my $redis = $self->redis;
    my $name  = $self->name;
    
    my $done = !$redis->llen( $name.'-reduced'  )
            && !$redis->get(  $name.'-reducing' )
            && !$redis->llen( $name.'-mapped'   )
            && !$redis->get(  $name.'-mapping'  )
            && !$redis->llen( $name.'-input'    )
    ;
    
    # TODO: This needs to be done in the Mapper and Reducer classes
    if ($done) {
        $redis->hdel( mapper  => $self->name );
        $redis->hdel( reducer => $self->name );
    }
    
    return $done;
}

sub next_result {
    my ($self) = @_;
    
    my $redis = $self->redis;
    my $name  = $self->name;
    
    while (1) {
        return undef if $self->done;
        
        MapReduce->debug( "Input queue:   %s", $redis->llen( $name.'-input'    ) );
        MapReduce->debug( "Mapped queue:  %s", $redis->llen( $name.'-mapped'   ) );
        MapReduce->debug( "Reduced queue: %s", $redis->llen( $name.'-reduced'  ) );
        MapReduce->debug( "Mapping?:      %s", $redis->get(  $name.'-mapping'  ) );
        MapReduce->debug( "Reducing?:     %s", $redis->get(  $name.'-reducing' ) );

        my $reduced = $redis->brpop( $self->name.'-reduced', 1);
        
        next if !defined $reduced;
        next if !defined $reduced->[1];
        
        my $value = thaw($reduced->[1]);
        
        die 'Reduced result is undefined?'
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

