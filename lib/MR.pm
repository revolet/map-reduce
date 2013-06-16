package MR;
use Moo;
use Storable qw(nfreeze thaw);
use B::Deparse;
use MR::Mapper;
use MR::Reducer;

# Do not change these. Rather, to enable logging,
# change the $LOGGING value to one of these variables.
our $DEBUG = 2;
our $INFO  = 1;
our $NONE  = 0;

# Enable / disable logging.
our $LOGGING = $ENV{MR_LOGGING} // $NONE;

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

has redis => (
    is       => 'ro',
    lazy     => 1,
    default  => sub { Redis::hiredis->new(utf8 => 0) },
);

sub input {
    my ($self, $input) = @_;
    
    my $redis = $self->redis;
    
    $redis->connect('127.0.0.1', 6379);
    $redis->select(9);
    
    $redis->lpush( $self->name.'-input', nfreeze($input) );
    
    MR->debug( "Pushed input '%s' => '%s' to %s->input.", $input->{key}, $input->{value}, $self->name );
    
    return $self;
}

sub run {
    my ($self) = @_;
    
    my $deparse = B::Deparse->new();
    my $mapper  = $deparse->coderef2text( $self->mapper  );
    my $reducer = $deparse->coderef2text( $self->reducer );
    my $redis   = $self->redis;
    
    MR->debug( "Mapper is '%s'",  $mapper );
    MR->debug( "Reducer is '%s'", $reducer );
    
    $redis->hset( mapper  => ( $self->name => $mapper  ) );
    $redis->hset( reducer => ( $self->name => $reducer ) );
    
    return $self;
}

sub done {
    my ($self) = @_;
    
    my $redis = $self->redis;
    my $name  = $self->name;
    
    return !$redis->llen( $name.'-input'    )
        && !$redis->llen( $name.'-mapped'   )
        && !$redis->llen( $name.'-reduced'  )
        && !$redis->get(  $name.'-mapping'  )
        && !$redis->get(  $name.'-reducing' )
    ;
}
sub next_result {
    my ($self) = @_;
    
    my $redis = $self->redis;
    my $name  = $self->name;
    
    while (1) {
        return undef if $self->done;

        MR->debug( "Input queue:   %s", $redis->llen( $name.'-input'    ) );
        MR->debug( "Mapped queue:  %s", $redis->llen( $name.'-mapped'   ) );
        MR->debug( "Reduced queue: %s", $redis->llen( $name.'-reduced'  ) );
        MR->debug( "Mapping?:      %s", $redis->get(  $name.'-mapping'  ) );
        MR->debug( "Reducing?:     %s", $redis->get(  $name.'-reducing' ) );

        my $reduced = $redis->brpop( $self->name.'-reduced', 1);
        
        next if !defined $reduced;
        next if !defined $reduced->[1];
        
        my $value = thaw($reduced->[1]);
        
        die 'Reduced result is undefined?'
            if !defined $value;
        
        return $value;
    }
}

1;

