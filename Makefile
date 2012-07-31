REPORTER = dot

test:
	@NODE_ENV=test \
	./node_modules/.bin/mocha \
		--reporter $(REPORTER) \
		test/*.js

coffee-test: 
	@NODE_ENV=test \
	./node_modules/.bin/mocha \
		--reporter $(REPORTER) \
		--compilers coffee
		test/*.coffee
	

.PHONY: coffee-test
