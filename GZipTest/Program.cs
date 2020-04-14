using System;
using System.Diagnostics;

namespace GZipTest
{
    class Program
    {

        static void Main(string[] args)
        {

            Console.WriteLine("Enter the filename to be compressed.");
            string fileName = Console.ReadLine();

            MyFile mf = new MyFile(fileName,  1);
            mf.Compress();
            
            MyFile.BeginDecompression(fileName + ".gzip");
            Console.ReadLine();
        }

    }
}
