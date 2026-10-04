SWAGGER_DIR = api/gen/openapiv2
DOCS_DIR = api/gen/docs
FINAL_SPEC = $(SWAGGER_DIR)/city.swagger.json

.PHONY: docs gen-proto merge-docs bundle-docs

proto: gen-proto

# То же, что проверяет CI: lint + актуальность api/gen
proto-check:
	buf lint
	buf generate
	@git add -N api/gen
	@git diff --exit-code -- api/gen || (echo "api/gen устарел: выполните make proto и закоммитьте"; exit 1)

# Сравнение с main (работает после слияния T008)
proto-breaking:
	buf breaking --against '.git#branch=main'

merge-docs: proto
	mkdir -p $(DOCS_DIR)
	@echo "Converting Swagger 2.0 to OpenAPI 3.0..."
	find $(SWAGGER_DIR) -name "*.swagger.json" -not -name "city.swagger.json" -exec npx -y swagger2openapi --patch --yaml --outfile {}.yaml {} \;

	@echo "Joining OpenAPI 3.0 files..."
	npx -y @redocly/cli join $(SWAGGER_DIR)/**/*.yaml -o $(FINAL_SPEC)

	find $(SWAGGER_DIR) -name "*.yaml" -delete

bundle-docs: merge-docs
	@echo "Bundling final HTML..."
	npx -y @redocly/cli build-docs $(FINAL_SPEC) -o $(DOCS_DIR)/index.html

docs: bundle-docs
	@echo "-------------------------------------------------------"
	@echo "Success! Documentation: $(DOCS_DIR)/index.html"
	@echo "-------------------------------------------------------"

monitor-nats:
	./deployments/scripts/nats/monitor.sh
