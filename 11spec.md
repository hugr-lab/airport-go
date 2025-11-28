# Extended features

1. Add parallel scalar function execution support

   - Add server configuration option `MaxParallelScalarFunctionCallsPerBatch` to control the maximum number of parallel scalar functions that can be executed simultaneously in one batch. Imagine the better naming.
   - Add function configuration option `EnableParallelExecution` to enable or disable parallel execution for specific scalar functions.
   - Update the function execution logic to respect the `MaxParallelScalarFunctionCallsPerBatch` limit when executing scalar functions in parallel.

2. Add support for scalar function timeout

   - Add function configuration option `Timeout` to specify a timeout for individual scalar function calls.
   - Update the function execution logic to enforce the specified timeout when executing scalar functions.

3. Add support for table function timeout:

   - Add table function configuration option `Timeout` to specify a timeout for table function calls.
   - Update the function execution logic to enforce the global timeout when executing table functions.
