using System;

namespace NRedisBloom.Shared
{
    public static class ObjectExtensions
    {
        /// <summary>
        /// Adds a value to the beginning of the sequence
        /// </summary>
        /// <param name="first">Value to prepend to source</param>
        /// <param name="source">A sequence of values</param>
        /// <typeparam name="T">Type of the elements</typeparam>
        /// <returns>A new sequence that begins with first</returns>
        internal static object[] PrependToArray<T>(this T first, params T[] source)
        {
            var destination = new object[source.Length + 1];

            destination[0] = first;

            Array.Copy(source, 0, destination, 1, source.Length);

            return destination;
        }
    }
}
