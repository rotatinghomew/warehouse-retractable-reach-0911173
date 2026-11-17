test:
	python3 -m unittest discover -s tests -v

build:
	python3 -m compileall -q reachsets tests

demo:
	python3 -m reachsets
