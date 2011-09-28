package ElasticSearch::Transport;

use strict;
use warnings FATAL => 'all';
use ElasticSearch::Util qw(throw build_error parse_params);
use URI();
use JSON();
use Encode qw(decode_utf8);
use Scalar::Util qw(openhandle);

our %Transport = (
    'http'     => 'ElasticSearch::Transport::HTTP',
    'httplite' => 'ElasticSearch::Transport::HTTPLite',
    'thrift'   => 'ElasticSearch::Transport::Thrift',
    'httptiny' => 'ElasticSearch::Transport::HTTPTiny',
    'curl'     => 'ElasticSearch::Transport::Curl',
    'aehttp'   => 'ElasticSearch::Transport::AEHTTP',
    'aecurl'   => 'ElasticSearch::Transport::AECurl',
);

#===================================
sub new {
#===================================
    my $class           = shift;
    my $params          = shift;
    my $transport_name  = delete $params->{transport} || 'http';
    my $transport_class = $Transport{$transport_name}
        or $class->throw(
        'Param',
        "Unknown transport '$transport_name'",
        { Available => \%Transport }
        );

    eval "require  $transport_class" or $class->throw( "Internal", $@ );

    my $self = bless {
        _JSON         => JSON->new(),
        _timeout      => 120,
        _max_requests => 10_000,
        _failed       => {},
        },
        $transport_class;

    my $servers = delete $params->{servers}
        || '127.0.0.1:' . $transport_class->default_port;

    $self->{_default_servers}
        = [ ref $servers eq 'ARRAY' ? @$servers : $servers ];

    for (qw(timeout max_requests no_refresh deflate)) {
        next unless exists $params->{$_};
        $self->$_( delete $params->{$_} );
    }
    $self->init($params);
    return $self;
}

#===================================
sub init { shift() }
#===================================

#===================================
sub request {
#===================================
    my $self          = shift;
    my $params        = shift;
    my $single_server = shift;

    my $args = $self->_tidy_params($params);
    $self->reset_failed_servers();

    my $json;
    while (1) {
        my $srvr = $single_server || $self->next_server;
        $self->log_request( $srvr, $args ) unless $single_server;

        $json = eval { $self->send_request( $srvr, $args ) || '{"ok":true}' }
            and last;

        my $error = $@ || 'Unknown error';
        next if !$single_server && $self->_should_retry( $srvr, $error );
        $error = $self->_handle_error( $srvr, $params, $error )
            or return;
        die $error;
    }
    return $self->_response( $json, $params, $single_server );

}

#===================================
sub _response {
#===================================
    my $self          = shift;
    my $response_json = shift;
    my $params        = shift;
    my $skip_log      = shift;

    my $as_json      = $params->{as_json};
    my $post_process = $params->{post_process};

    my $result;
    $result = $self->JSON->decode($response_json)
        unless $as_json && !$post_process && $skip_log;

    $self->log_response( $result || $response_json )
        unless $skip_log;

    if ($post_process) {
        $result = $post_process->($result);
        if ($as_json) {
            $response_json = $self->JSON->encode($result);
            $result        = undef;
        }
    }

    return $as_json ? $response_json : $result;
}

#===================================
sub _should_retry {
#===================================
    my $self   = shift;
    my $server = shift;
    my $error  = shift;

    return unless $error->isa('ElasticSearch::Error::Connection');

    warn "Error connecting to '$server' : "
        . ( $error->{-text} || 'Unknown' ) . "\n\n";

    $self->no_refresh
        ? $self->_remove_server($server)
        : $self->{_refresh_in} = 0;

    return 1;
}

#===================================
sub _handle_error {
#===================================
    my $self   = shift;
    my $server = shift;
    my $params = shift;
    my $error  = shift || 'Unknown error';

    $error = build_error( $self, 'Request', $error, { request => $params } )
        unless ref $error;

    return
        if $error->isa('ElasticSearch::Error::Missing')
            && $params->{qs}{ignore_missing};

    $error->{-vars}{request} = $params;
    if ( my $raw = $error->{-vars}{content} ) {
        my $content = eval { $self->JSON->decode($raw) } || $raw;
        $self->log_response($content);
        if ( ref $content and $content->{error} ) {
            $error->{-text} = $content->{error};
            $error->{-vars}{error_trace} = $content->{error_trace}
                if $content->{error_trace};
            delete $error->{-vars}{content};
        }
    }
    return $error;
}

#===================================
sub _tidy_params {
#===================================
    my $self   = shift;
    my $params = shift;

    $params->{method} ||= 'GET';
    $params->{cmd}    ||= '/';
    $params->{qs}     ||= {};

    my $data = $params->{data};
    $data
        = ref $data eq 'SCALAR'
        ? $$data
        : $self->JSON->encode($data)
        if $data;

    return { data => $data, map { $_ => $params->{$_} } qw(method cmd qs) };
}

#===================================
sub refresh_servers {
#===================================
    my $self = shift;

    $self->{_refresh_in} = 0;
    delete $self->{_current_server};

    my %servers = map { $_ => 1 }
        ( @{ $self->servers }, @{ $self->default_servers } );

    my @all_servers = keys %servers;
    my $protocol    = $self->protocol;

    foreach my $server (@all_servers) {
        next unless $server;

        my $nodes
            = eval { $self->request( { cmd => '/_cluster/nodes' }, $server ) }
            or next;

        my @servers = grep {$_}
            map {m{/([^]]+)}}
            map {
                   $_->{ $protocol . '_address' }
                || $_->{ $protocol . 'Address' }
                || ''
            } values %{ $nodes->{nodes} };
        next unless @servers;

        $self->{_refresh_in} = $self->max_requests - 1;
        return $self->servers( \@servers );
    }

    $self->throw(
        'NoServers',
        "Could not retrieve a list of active servers:\n$@",
        { servers => \@all_servers }
    );
}

#===================================
sub next_server {
#===================================
    my $self = shift;
    unless ( $self->{_refresh_in}-- ) {
        if ( $self->no_refresh ) {
            $self->servers( $self->default_servers );
            $self->{_refresh_in} = $self->max_requests - 1;
            $self->reset_failed_servers();
        }
        else {
            $self->refresh_servers;
        }
    }

    my @servers = @{ $self->servers };

    unless (@servers) {
        my $failed = $self->{_failed};
        @servers = grep { !$failed->{$_} } @{ $self->default_servers };
        unless (@servers) {
            $self->{_refresh_in} = 0;
            $self->throw(
                "NoServers",
                "No servers available:\n",
                { servers => $self->default_servers }
            );
        }

    }

    my $next = shift(@servers);

    $self->{_current_server} = { $$ => $next };
    $self->servers( @servers, $next );
    return $next;
}

#===================================
sub _remove_server {
#===================================
    my $self   = shift;
    my $server = shift;
    $self->{_failed}{$server}++;
    my @servers = grep { $_ ne $server } @{ $self->servers };
    $self->servers( \@servers );
}

#===================================
sub reset_failed_servers {
#===================================
    my $self = shift;
    $self->{_failed} = {};
}

#===================================
sub current_server {
#===================================
    my $self = shift;
    return $self->{_current_server}{$$} || $self->next_server;
}

#===================================
sub servers {
#===================================
    my $self = shift;
    if (@_) {
        $self->{_servers} = ref $_[0] eq 'ARRAY' ? shift : [@_];
    }
    return $self->{_servers} ||= [];
}

#===================================
sub max_requests {
#===================================
    my $self = shift;
    if (@_) {
        $self->{_max_requests} = shift;
    }
    return $self->{_max_requests} || 0;
}

#===================================
sub default_servers { shift->{_default_servers} }
#===================================

#===================================
sub http_uri {
#===================================
    my $self   = shift;
    my $server = shift;
    my $cmd    = shift;
    $cmd = '' unless defined $cmd;
    my $uri = URI->new( 'http://' . $server . $cmd );
    $uri->query_form(shift) if $_[0];
    return $uri->as_string;
}

#===================================
sub inflate {
#===================================
    my $self = shift;
    my $content = shift;
    my $output;
        require IO::Uncompress::Inflate;

        no warnings 'once';
        IO::Uncompress::Inflate::inflate( \$content, \$output, Transparent => 0 )
            or die "Couldn't inflate response: "
                . $IO::Uncompress::Inflate::InflateError;
    return $output;
}

#===================================
sub timeout {
#===================================
    my $self = shift;
    if (@_) {
        $self->{_timeout} = shift;
        $self->clear_clients;
    }
    return $self->{_timeout} || 0;
}

#===================================
sub deflate {
#===================================
    my $self = shift;
    if (@_) {
        $self->{_deflate} = shift;
        $self->clear_clients;
    }
    return $self->{_deflate} || 0;
}

#===================================
sub no_refresh {
#===================================
    my $self = shift;
    if (@_) {
        $self->{_no_refresh} = !!shift();
    }
    return $self->{_no_refresh} || 0;
}

#===================================
sub trace_calls {
#===================================
    my $self = shift;
    if (@_) {
        delete $self->{_log_fh};
        $self->{_trace_calls} = shift;
        $self->JSON->pretty( !!$self->{_trace_calls} );

    }
    return $self->{_trace_calls};
}

#===================================
sub _log_fh {
#===================================
    my $self = shift;
    unless ( exists $self->{_log_fh}{$$} ) {
        my $log_fh;
        if ( my $file = $self->trace_calls ) {
            $file = \*STDERR if $file eq 1;
            my $open_mode = '>>';
            if ( openhandle($file) ) {
                $open_mode = '>>&';
            }
            else {
                $file .= ".$$";
            }
            open $log_fh, $open_mode, $file
                or $self->throw( 'Internal',
                "Couldn't open '$file' for trace logging: $!" );
            binmode( $log_fh, ':utf8' );
            $log_fh->autoflush(1);
        }
        $self->{_log_fh}{$$} = $log_fh;
    }
    return $self->{_log_fh}{$$};
}

#===================================
sub log_request {
#===================================
    my $self   = shift;
    my $log    = $self->_log_fh or return;
    my $server = shift;
    my $params = shift;

    my $data = $params->{data};
    if ( defined $data and $data ne "{}\n" ) {
        $data =~ s/'/\\u0027/g;
        $data = " -d '\n${data}'";
    }
    else {
        $data = '';
    }

    printf $log (
        "# [%s] Protocol: %s, Server: %s\n",
        scalar localtime(),
        $self->protocol, ${server}
    );
    my %qs = ( %{ $params->{qs} }, pretty => 1 );
    my $uri = $self->http_uri( $server, $params->{cmd}, \%qs );

    my $method = $params->{method};
    print $log "curl -X$method '$uri' ${data}\n\n";
}

#===================================
sub log_response {
#===================================
    my $self    = shift;
    my $log     = $self->_log_fh or return;
    my $content = shift;
    my $out     = ref $content ? $self->JSON->encode($content) : $content;
    my @lines   = split /\n/, $out;
    printf $log ( "# [%s] Response:\n", scalar localtime() );
    while (@lines) {
        my $line = shift @lines;
        if ( length $line > 65 ) {
            my ($spaces) = ( $line =~ /^(?:> )?(\s*)/ );
            $spaces = substr( $spaces, 0, 20 ) if length $spaces > 20;
            unshift @lines, '> ' . $spaces . substr( $line, 65 );
            $line = substr $line, 0, 65;
        }
        print $log "# $line\n";
    }
    print $log "\n";
}

#===================================
sub clear_clients {
#===================================
    my $self = shift;
    delete $self->{_client};
}

#===================================
sub JSON { shift()->{_JSON} }
#===================================

my %Statuses = (
    100 => 'CONT',
    101 => 'SWITCHING_PROTOCOLS',
    200 => 'OK',
    201 => 'CREATED',
    202 => 'ACCEPTED',
    203 => 'NON_AUTHORITATIVE_INFORMATION',
    204 => 'NO_CONTENT',
    205 => 'RESET_CONTENT',
    206 => 'PARTIAL_CONTENT',
    207 => 'MULTI_STATUS',
    300 => 'MULTIPLE_CHOICES',
    301 => 'MOVED_PERMANENTLY',
    302 => 'FOUND',
    303 => 'SEE_OTHER',
    304 => 'NOT_MODIFIED',
    305 => 'USE_PROXY',
    307 => 'TEMPORARY_REDIRECT',
    400 => 'BAD_REQUEST',
    401 => 'UNAUTHORIZED',
    402 => 'PAYMENT_REQUIRED',
    403 => 'FORBIDDEN',
    404 => 'NOT_FOUND',
    405 => 'METHOD_NOT_ALLOWED',
    406 => 'NOT_ACCEPTABLE',
    407 => 'PROXY_AUTHENTICATION',
    408 => 'REQUEST_TIMEOUT',
    409 => 'CONFLICT',
    410 => 'GONE',
    411 => 'LENGTH_REQUIRED',
    412 => 'PRECONDITION_FAILED',
    413 => 'REQUEST_ENTITY_TOO_LARGE',
    414 => 'REQUEST_URI_TOO_LONG',
    415 => 'UNSUPPORTED_MEDIA_TYPE',
    416 => 'REQUESTED_RANGE_NOT_SATISFIED',
    417 => 'EXPECTATION_FAILED',
    422 => 'UNPROCESSABLE_ENTITY',
    423 => 'LOCKED',
    424 => 'FAILED_DEPENDENCY',
    500 => 'INTERNAL_SERVER_ERROR',
    501 => 'NOT_IMPLEMENTED',
    502 => 'BAD_GATEWAY',
    503 => 'SERVICE_UNAVAILABLE',
    504 => 'GATEWAY_TIMEOUT',
    506 => 'INSUFFICIENT_STORAGE',
);

#===================================
sub http_status {
#===================================
    my $code = $_[1] || 0;
    return $Statuses{$code} || 'Unknown code ' . $code;
}

#===================================
sub register {
#===================================
    my $class = shift;
    my $name  = shift
        || $class->throw( 'Param',
        'No transport name passed to register_transport()' );
    my $module = shift
        || $class->throw( 'Param',
        'No module name passed to register_transport()' );
    return $Transport{$name} = $module;
}

=head1 NAME

ElasticSearch::Transport - Base class for communicating with ElasticSearch

=head1 DESCRIPTION

ElasticSearch::Transport is a base class for the modules which communicate
with the ElasticSearch server.

It handles failover to the next node in case the current node closes the
connection. All requests are round-robin'ed to all live servers.

On the first request and every C<max_requests> after that (default 10,000),
the list of live nodes is automatically refreshed.  This can be disabled
by setting C<max_requests> to C<0>.

Regardless of the C<max_requests> setting, a list of live nodes will still be
retrieved on the first request.  This may not be desirable behaviour
if, for instance, you are connecting to remote servers which use internal
IP addresses, or which don't allow remote C<nodes()> requests.

If you want to disable this behaviour completely, set C<no_refresh> to C<1>,
in which case the transport module will round robin through the
C<servers> list only. Failed nodes will be removed from the list
(but added back in every C<max_requests> or when all nodes have failed):

Currently, the available backends are:

=over

=item * C<http> (default)

Uses L<LWP> to communicate using HTTP. See L<ElasticSearch::Transport::HTTP>

=item * C<httplite>

Uses L<HTTP::Lite> to communicate using HTTP.
See L<ElasticSearch::Transport::HTTPLite>

=item * C<httptiny>

Uses L<HTTP::Tiny> to communicate using HTTP.
See L<ElasticSearch::Transport::HTTPTiny>

=item * C<curl>

Uses L<WWW::Curl> and thus L<libcurl|http://curl.haxx.se/libcurl/>
to communicate using HTTP. See L<ElasticSearch::Transport::Curl>

=item * C<aehttp>

Uses L<AnyEvent::HTTP> to communicate asynchronously using HTTP.
See L<ElasticSearch::Transport::AEHTTP>

=item * C<aecurl>

Uses L<AnyEvent::Curl::Multi> (and thus L<libcurl|http://curl.haxx.se/libcurl/>)
to communicate asynchronously using HTTP. See L<ElasticSearch::Transport::AECurl>

=item * C<thrift>

Uses C<thrift>  to communicate using a compact binary protocol over sockets.
See L<ElasticSearch::Transport::Thrift>. You need to have the
C<transport-thrift> plugin installed on your ElasticSearch server for this
to work.

=back

You shouldn't need to talk to the transport modules directly - everything
happens via the main L<ElasticSearch> class.

=cut

=head1 SYNOPSIS


    use ElasticSearch;
    my $e = ElasticSearch->new(
        servers     => 'search.foo.com:9200',
        transport   => 'httplite',
        timeout     => '10',
        no_refresh  => 0 | 1,
        deflate     => 0 | 1,
    );

    my $t = $e->transport;

    $t->max_requests(5)             # refresh_servers every 5 requests
    $t->protocol                    # eg 'http'
    $t->next_server                 # next node to use
    $t->current_server              # eg '127.0.0.1:9200' ie last used node
    $t->default_servers             # seed servers passed in to new()

    $t->servers                     # eg ['192.168.1.1:9200','192.168.1.2:9200']
    $t->servers(@servers);          # set new 'live' list

    $t->refresh_servers             # refresh list of live nodes

    $t->clear_clients               # clear all open clients

    $t->no_refresh(0|1)             # don't retrieve the live node list
                                    # instead, use just the nodes specified

    $t->deflate(0|1);               # should ES deflate its responses
                                    # useful if ES is on a remote network

    $t->register('foo',$class)      # register new Transport backend

=head1 WHICH TRANSPORT SHOULD YOU USE

Although the C<thrift> interface has the right buzzwords (binary, compact,
sockets), the generated Perl code is very slow. Until that is improved, I
recommend one of the C<http> backends instead.

The HTTP backends in increasing order of speed are:

=over

=item *

C<http> - L<LWP> based

=item *

C<httplite> - L<HTTP::Lite> based, about 30% faster than C<http>

=item *

C<httptiny> - L<HTTP::Tiny> based, about 1% faster than C<httplite>

=item *

C<curl> - L<WWW::Curl> based, about 60% faster than C<httptiny>!

=back

See also:
L<http://www.elasticsearch.org/guide/reference/modules/http.html>
and L<http://www.elasticsearch.org/guide/reference/modules/thrift.html>

=head1 SUBCLASSING TRANSPORT

If you want to add a new transport backend, then these are the methods
that you should subclass:

=head2 init()

    $t->init($params)

By default, a no-op. Receives a HASH ref with the parameters passed in to
C<new()>, less C<servers>, C<transport> and C<timeout>.

Any parameters specific to your module should be deleted from C<$params>

=head2 send_request()

    $json = $t->send_request($server,$params)

    where $params = {
        method  => 'GET',
        cmd     => '/_cluster',
        qs      => { pretty => 1 },
        data    => '{ "foo": "bar"}',
    }

This must be overridden in the subclass - it is the method called to
actually talk to the server.

See L<ElasticSearch::Transport::HTTP> for an example implementation.

=head2 protocol()

    $t->protocol

This must return the protocol in use, eg C<"http"> or C<"thrift">. It is
used to extract the list of bound addresses from ElasticSearch, eg
C<http_address> or C<thrift_address>.

=head2 client()

    $client = $t->client($server)

Returns the client object used in L</"send_request()">. The server param
will look like C<"192.168.5.1:9200">. It should store its clients in a PID
specific slot in C<< $t->{_client} >> as C<clear_clients()> deletes
this key.

See L<ElasticSearch::Transport::HTTP/"client()"> and
L<ElasticSearch::Transport::Thrift/"client()">
for an example implementation.

=head1 Registering your Transport backend

You can register your Transport backend as follows:

    BEGIN {
        ElasticSearch::Transport->register('mytransport',__PACKAGE__);
    }

=head1 SEE ALSO

=over

=item * L<ElasticSearch>

=item * L<ElasticSearch::Transport::HTTP>

=item * L<ElasticSearch::Transport::HTTPLite>

=item * L<ElasticSearch::Transport::HTTPTiny>

=item * L<ElasticSearch::Transport::Curl>

=item * L<ElasticSearch::Transport::AEHTTP>

=item * L<ElasticSearch::Transport::AECurl>

=item * L<ElasticSearch::Transport::Thrift>

=back

=head1 LICENSE AND COPYRIGHT

Copyright 2010 - 2011 Clinton Gormley.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;
