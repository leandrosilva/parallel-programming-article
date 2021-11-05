using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ParallelProgrammingArticle
{
    /// <summary>
    /// Source code for the article "Tudo ao mesmo tempo agora com C#" originally published
    /// at https://medium.com/pricefy-labs.
    /// 
    /// Please, do not mind the whole Console.WriteLine kind of thing that you will see below. I know
    /// it's all about I/O and everything but this is meant to education only. Chill out folks :)
    /// 
    /// Oh and btw, I recommend you to comment off all those Console.WriteLine of an example method
    /// and see how fast it completes.
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Parallel Programming in .NET\n");

            // "Static" Loop-based Parallelism
            //

            //Execute(ParallelFor);
            //Execute(ParallelForEach);
            //Execute(ParallelInvoke);

            // "Dynamic" Parent & Children Parallelism
            //

            //Execute(ParallelParentAndChildren);

            Console.WriteLine("\nPress any key to quit");
            Console.ReadKey();
            Console.WriteLine("Bye bye");
        }

        #region "Static" Loop-based Parallelism

        private static void ParallelFor()
        {
            var numberOfIterations = 1000;

            var min = 0;
            var max = 100_000_000;

            var random = new Random();
            var breakIndex = random.Next(1, numberOfIterations);

            Console.WriteLine($"Generating random numbers from {min} to {max} over {numberOfIterations} iterations");
            Console.WriteLine($"Random break index: {breakIndex}");

            var result = Parallel.For(1, numberOfIterations, (i, state) =>
            {
                Console.WriteLine($"- Iteration #{i} > Begin at thread #{Thread.CurrentThread.ManagedThreadId}, task #{Task.CurrentId}");

                // Has .Break() been called by another parallel iteration?
                if (state.ShouldExitCurrentIteration)
                {
                    // Is this current iteration greater then the one where .Break() was invoked?
                    if (state.LowestBreakIteration < i)
                    {
                        Console.WriteLine($"- Iteration #{i} > Will exit now <-----------");
                        return;
                    }
                }

                int num;

                // A naive lock for educative purpose only
                lock (random)
                {
                    num = random.Next(min, max);
                }

                // If it got to the break index, invokes .Break() to prevent further iterations
                if (i == breakIndex)
                {
                    Console.WriteLine($"- Iteration #{i} > Got to break index <-----------");
                    state.Break();
                }

                Console.WriteLine($"- Iteration #{i} > End: {num}");
            });

            if (result.LowestBreakIteration.HasValue)
                Console.WriteLine($"Lowest break iteration? {result.LowestBreakIteration}");
        }

        private static void ParallelForEach()
        {
            var numberOfIterations = 1000;

            var min = 0;
            var max = 100_000_000;

            // Will cancel the operation on a random time basis; it might complete sometimes
            // depending on the host machine
            var timeInMs = 0;
            var cts = new CancellationTokenSource();
            Task.Run(async () =>
            {
                timeInMs = new Random().Next(100, 1000);
                await Task.Delay(timeInMs);

                Console.WriteLine($"- Will cancel after {timeInMs} ms <-----------");
                cts.Cancel();
            });

            Console.WriteLine($"Generating random numbers from {min} to {max} over {numberOfIterations} iterations");

            try
            {
                Parallel.ForEach(
                    GenerateRandomNumbers(numberOfIterations, min, max),
                    new ParallelOptions
                    {
                        CancellationToken = cts.Token,
                        //MaxDegreeOfParallelism = 100 // <-- you can play with this number to get a grasp of how this works;
                        //                             //     essentially, the max viable degree of parallelism is the number
                        //                             //     of cores available; try a few numbers and watch it for time to
                        //                             //     complete the work
                    },
                    (num, state, i) =>
                    {
                        Console.WriteLine($"- Iteration #{i} > Begin at thread #{Thread.CurrentThread.ManagedThreadId}, task #{Task.CurrentId}");
                        Console.WriteLine($"- Iteration #{i} > End: {num}");
                    });
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"Parallel task was canceled by a CancellationToken after {timeInMs} ms");
            }
        }

        private static void ParallelInvoke()
        {
            var numberOfIterations = 1000;

            var min = 0;
            var max = 100_000_000;

            Console.WriteLine($"Generating random numbers from {min} to {max} over {numberOfIterations} iterations");

            var nums = GenerateRandomNumbers(numberOfIterations, min, max).Select(num => (long)num).ToArray();

            // With Parallel LINQ it is a piece of cake -- fast as f*k
            var originalSum = nums.AsParallel().Sum();

            Parallel.Invoke(
                () => ExpensivePlusOne(ref nums, 0, nums.Length / 2),
                () => ExpensiveMinusTwo(ref nums, nums.Length / 2, nums.Length)
            );

            var newSum = nums.AsParallel().Sum();

            Console.WriteLine($"Sum of all random generated numbers are {originalSum} (original) and {newSum} (new) [{originalSum - newSum} less]");
        }

        #endregion

        #region "Dynamic" Parent & Children Parallelism

        private static void ParallelParentAndChildren()
        {
            var numberOfIterations = 1000;

            var min = 0;
            var max = 100_000_000;

            Console.WriteLine($"Generating random numbers from {min} to {max} over {numberOfIterations} iterations");

            var nums = GenerateRandomNumbers(numberOfIterations, min, max).Select(num => (long)num).ToArray();

            var task = Task.Factory.StartNew(
                () => ProcessNumbers(nums),
                CancellationToken.None,
                TaskCreationOptions.None,
                TaskScheduler.Default
            );

            Console.WriteLine("Spawned parallel process of numbers and will wait until completion");

            task.Wait();
        }

        /// <summary>
        /// Let's pretend it is so freaking CPU-intensive.
        /// </summary>
        private static void ProcessNumbers(long[] nums)
        {
            var odds = new ConcurrentBag<long>();
            var evens = new ConcurrentBag<long>();

            Console.WriteLine("Will divide odds / evens in parallel");

            Parallel.ForEach(nums, (num) =>
            {
                if (num % 2 != 0)
                    odds.Add(num);
                else
                    evens.Add(num);
            });

            Task.Factory.StartNew(
                () => ExpensiveWorkWithOdds(odds.ToArray()),
                CancellationToken.None,
                TaskCreationOptions.AttachedToParent,  // <-- pay attention at here
                TaskScheduler.Default
            );

            Console.WriteLine("Spawned parallel expensive work on odds");

            Task.Factory.StartNew(
                () => ExpensiveWorkWithEvens(evens.ToArray()),
                CancellationToken.None,
                TaskCreationOptions.AttachedToParent,  // <-- pay attention at here
                TaskScheduler.Default
            );

            Console.WriteLine("Spawned parallel expensive work on evens");
        }

        #endregion

        #region Helper methods

        /// <summary>
        /// Generator method for random numbers.
        /// </summary>
        private static IEnumerable<int> GenerateRandomNumbers(int numberOfIterations, int min, int max)
        {
            var random = new Random();

            for (int i = 1; i <= numberOfIterations; i++)
            {
                int num;

                // A naive lock for educative purpose only
                lock (random)
                {
                    num = random.Next(min, max);
                }

                yield return num;
            }
        }

        /// <summary>
        /// Let's pretend it is so freaking CPU-intensive.
        /// </summary>
        private static void ExpensivePlusOne(ref long[] nums, int begin, int end)
        {
            for (int i = begin; i < end; i++)
            {
                nums[i] += 1;
            }

            Console.WriteLine("Expensive plus one are done");
        }

        /// <summary>
        /// Let's pretend it is so freaking CPU-intensive.
        /// </summary>
        private static void ExpensiveMinusTwo(ref long[] nums, int begin, int end)
        {
            for (int i = begin; i < end; i++)
            {
                nums[i] -= 2;
            }

            Console.WriteLine("Expensive minus one are done");
        }

        /// <summary>
        /// Let's pretend it is so freaking CPU-intensive.
        /// </summary>
        private static void ExpensiveWorkWithOdds(long[] odds)
        {
            Parallel.ForEach(odds, (num, state, i) => Math.Sign(num));
            Parallel.ForEach(odds, (num, state, i) => Math.Cos(num));

            Console.WriteLine("Expensive work with odds in parallel are done");
        }

        /// <summary>
        /// Let's pretend it is so freaking CPU-intensive.
        /// </summary>
        private static void ExpensiveWorkWithEvens(long[] evens)
        {
            Parallel.ForEach(evens, (num, state, i) => Math.Sqrt(num));

            Console.WriteLine("Expensive work with evens in parallel are done");
        }

        /// <summary>
        /// Helper method to execute a method that shows an example of parallel programming.
        /// </summary>
        private static void Execute(Action action)
        {
            var name = action.Method.Name;

            Console.WriteLine($"---\n> {name} > Start\n");

            var sw = Stopwatch.StartNew();

            action();

            sw.Stop();

            Console.WriteLine($"\n> {name} > Finished in {sw.ElapsedMilliseconds} ms\n---\n");
        }

        #endregion
    }
}
