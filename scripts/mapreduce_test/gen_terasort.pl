#!/usr/bin/perl

#exit(66);

sub generate_random_string
{
    my $length_of_randomstring=shift;# the length of 
             # the random string to generate

    my @chars=('a'..'z','A'..'Z','0'..'9','_');
    my $random_string;
    foreach (1..$length_of_randomstring) 
    {
        # rand @chars will generate a random 
        # number between 0 and scalar @chars
        $random_string.=$chars[rand @chars];
    }
    return $random_string;
}

$buf = ' ';
while($buf) {
    sysread STDIN, $buf, 1;
}

for ($i = 0; $i < $ARGV[0]; ++$i) {
    $key = generate_random_string(10);
    $value = generate_random_string(90);
    print "$key\t\t$value\n";
}
