start_server {
	tags {"dsf"}
} {
	proc create_dsf {key entries} {
        r del $key
        foreach entry $entries { r dsfadd $key $entry }
    }
    
    test {DSFADD, DSFCARD} {
        create_dsf mydsf {foo}
        assert_encoding hashtable mydsf
        assert_equal 1 [r dsfcard mydsf]
    }
    
    test {DSFADD, DSFARECOMEMBERS DSFUNION, DSFCARD, DSFSIZE} {
        create_dsf mydsf {foo bar pooh}
        assert_encoding hashtable mydsf
        
        # Initial state of DSF: all members are singletons.
        assert_equal 3 [r dsfcard mydsf]
        assert_equal 3 [r dsfsize mydsf]
        assert_equal 0 [r dsfarecomembers foo bar]
        assert_equal 0 [r dsfarecomembers foo pooh]
        assert_equal 0 [r dsfarecomembers bar pooh]
        
        # After single merge...
        assert_equal 1 [r dsfunion foo bar]
        assert_equal 2 [r dsfcard mydsf]
        assert_equal 3 [r dsfsize mydsf]
        assert_equal 1 [r dsfarecomembers foo bar]
        assert_equal 0 [r dsfarecomembers foo pooh]
        assert_equal 0 [r dsfarecomembers bar pooh]
        
        # After remaining merge...
        assert_equal 1 [r dsfunion bar pooh]
        assert_equal 1 [r dsfcard mydsf]
        assert_equal 3 [r dsfsize mydsf]
        assert_equal 1 [r dsfarecomembers foo bar]
        assert_equal 1 [r dsfarecomembers foo pooh]
        assert_equal 1 [r dsfarecomembers bar pooh]
    }
    
    test {DSFADD, DSFREM, DSFSIZE} {
        create_dsf mydsf {foo bar}
        assert_encoding hashtable mydsf
        assert_equal 2 [r dsfsize mydsf]
        assert_equal 1 [r dsfrem mydsf foo]
        assert_equal 1 [r dsfsize mydsf]
        assert_equal 1 [r dsfrem mydsf bar]
        assert_equal 0 [r dsfsize mydsf]
    }
}