### reading csv's

How to benchmark:

- There is a test in the `main_test.go` file containing a benchmark loop
- run the command `go test -bench . -run ^$` to run only the benchmark test
- you can make the bench tool run more iterations of the benchmark using a flag on the command:
`go test -bench .  -benchtime10x -run ^$`

- to profile the program run `go test -bench . -benchtime=10x -run ^$ -cpuprofile cpu00.pprof`
The above command will generate two files that will be used for profiling the program

To use the generated files for profiling you have to run:
`go tool pprof cpu00.pprof`

This will start an interactive cli session where you can view more details about the functions running in the program
Inside that interactive cli session you can use the `top` and `list` commands to see details of the functions that are running