compile:
	ERL_LIBS=deps erlc -o ebin src/*.erl

uzip:
	@unzip -d deps deps/rabbit_common-3.6.9.ez
	@unzip -d deps deps/amqp_client-3.6.9.ez
