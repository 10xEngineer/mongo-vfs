REPORTER = dot

test:
	@NODE_ENV=test \
	./node_modules/.bin/mocha \
		--reporter $(REPORTER) \
		test/*.js

auto-test: 
	@NODE_ENV=test \
	nodemon -w src/ -w test/ \
	./node_modules/.bin/mocha \
		--reporter $(REPORTER) \
		--compilers coffee:coffee-script \
		test/*.coffee
	

.PHONY: test
