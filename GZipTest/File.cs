using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

namespace GZipTest
{
    class MyFile
    {
        class Block
        {
            public Block(uint id, byte [] data)
            {
                this._id = id;
                this._data = data;
                this._isDone = false;
            }
            ~Block()
            {
                this._data = null;
            }
            public uint ID {  get { return _id; } }
            public bool IsDone { get { return _isDone; } }

            public void Finish()
            {
                // atomic update of isDone
                this._isDone = true;
            }

            public byte [] GetData { get { return _data; } }

            public void processBlock()
            {
                using (MemoryStream ms = new MemoryStream(_data.Length))
                {
                    using (DeflateStream dfl = new DeflateStream(ms, CompressionLevel.Fastest, false))
                    {
                        dfl.Write(_data, 0, _data.Length);
                    }
                    // store compressed data when deflater stream flushed
                    this._data = ms.ToArray();
                }
            }

            private uint _id;
            private byte[] _data;
            private bool _isDone;
        }

        public MyFile(string fileName, uint blockSize, uint maxBlocks = 1000)
        {
            this._fileName = fileName;
            this._blockSize = blockSize;
            this._maxBlocks = maxBlocks;
            this._chunkSize = maxBlocks / 2;
            this._nWorkers = Environment.ProcessorCount;
            this._fileSize = new System.IO.FileInfo(fileName).Length;
            this._nBlocks = Convert.ToInt32(Math.Ceiling(this._fileSize / (double)_blockSize));
            this._crc32 = 0; 

            _inputQueue = new Queue<Block>();
            _outputDict = new Dictionary<uint, List<Block>>();
            // construct synchro events
            readerEvent = new ManualResetEventSlim(false);
            writerEvent = new ManualResetEventSlim(false);

            int nMaxBlocks = (int)_maxBlocks;
            _semEmpty = new SemaphoreSlim(nMaxBlocks, nMaxBlocks);
            _semFull = new SemaphoreSlim(0, nMaxBlocks);
        }
        ~MyFile()
        {
            _outputDict = null;
            _inputQueue = null;
            _semEmpty.Dispose();
            _semFull.Dispose();
            readerEvent.Dispose();
            writerEvent.Dispose();
        }

        private Queue<Block> _inputQueue;
        // key will be chunk ID
        private Dictionary<uint, List<Block>> _outputDict;

        private int _nBlocks;
        private int _nWorkers;
        private uint _chunkSize;
        private long _fileSize;
        private string _fileName;
        private uint _maxBlocks;
        private uint _blockSize;
        private uint _crc32;

        private SemaphoreSlim _semEmpty, _semFull;
        private ManualResetEventSlim readerEvent;
        private ManualResetEventSlim writerEvent;
        // driver for reading thread
        private void ReaderThread()
        {
            using (var fs = new FileStream(_fileName, FileMode.Open, FileAccess.Read))
            {
                uint blockID = 0;
                bool EOF = false;
                while (!EOF)
                {
                    _semEmpty.Wait();

                    byte[] data = new byte[_blockSize];
                    int offset = 0;
                    int nRead;
                    while (offset < _blockSize && (nRead = fs.Read(data, offset, (int)_blockSize - offset)) != 0)
                    {
                        offset += nRead;
                    }
                    if (offset < _blockSize)
                    {
                        Console.WriteLine($"EOF reached: blockID: {blockID}, thread = {Thread.CurrentThread.Name}");
                        EOF = true;
                        Array.Resize(ref data, offset);
                    }

                    var block = new Block(blockID++, data);
                    // update crc32
                    _crc32 = Force.Crc32.Crc32Algorithm.Append(_crc32, data);

                    lock (_inputQueue)
                    {
                        _inputQueue.Enqueue(block);
                    }

                    _semFull.Release();

                    if (EOF)
                    {
                        Block dummyBlock = new Block(0, null);
                        for (int i = 0; i < _nWorkers; ++i)
                        {
                            _semEmpty.Wait();
                            lock (_inputQueue)
                            {
                                _inputQueue.Enqueue(dummyBlock);
                            }
                            _semFull.Release();
                        }
                    }
                    // signalize other threads to finish
                }
            }
        }

        private byte[] PrepareHeader()
        {
            byte[] fileNameConv = Encoding.GetEncoding("ISO-8859-1").GetBytes(_fileName.ToCharArray());
            // http://www.zlib.org/rfc-gzip.html?fbclid=IwAR1O5VUAfibD272UxWPL9_vHb7SHrEDIvbCwMeGPN9LMiAGQ30AjdpddDss
            byte[] unixTimestamp = BitConverter.GetBytes((Int32)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds);

            byte[] header1 = { 0x1f, 0x8b, 0x08, 0x08, unixTimestamp[0], unixTimestamp[1], unixTimestamp[2], unixTimestamp[3], 0x04, 0x00 };
            byte[] result = new byte[header1.Length + fileNameConv.Length + 1];
            Buffer.BlockCopy(header1, 0, result, 0, header1.Length);
            Buffer.BlockCopy(fileNameConv, 0, result, header1.Length, fileNameConv.Length);
            result[result.Length - 1] = 0x00;
            return result;
        }

        private void WriterThread()
        {
            string outputFileName = String.Concat(_fileName, ".gzip");
            if (File.Exists(outputFileName))
            {
                File.Delete(outputFileName);
            }

            using (FileStream fs = new FileStream(outputFileName, FileMode.CreateNew))
            {
                // get gzip header
                byte[] header = PrepareHeader();
                fs.Write(header, 0, header.Length);
                int blockID = 0;
                uint chunkID = 0;
                uint refChunkID = uint.MaxValue;
                List<Block> curChunk = null;
                while (true)
                {
                    if (chunkID == refChunkID)
                    {
                        if (curChunk[blockID] != null && curChunk[blockID].IsDone)
                        {
                         //   Console.WriteLine($"Writing ID {blockID}");
                            byte[] data = curChunk[blockID++].GetData;
                            fs.Write(data, 0, data.Length);

                            if (blockID >= _chunkSize)
                            {
                                lock (_outputDict)
                                {
                                    _outputDict.Remove(chunkID);
                                }
                                curChunk = null;
                                ++chunkID;
                                blockID = 0;
                            }
                            // end when last block processed
                            if (chunkID * _chunkSize + blockID >= _nBlocks)
                            {
                                Console.WriteLine($"All block processed {chunkID * _chunkSize + blockID}, thread = {Thread.CurrentThread.Name}");
                                break;
                            }
                        }
                        else
                        {
                            writerEvent.Wait();
                        }
                    }
                    else
                    {
                        bool gotChunkRef = false;
                        lock (_outputDict)
                        {
                            gotChunkRef = _outputDict.TryGetValue(chunkID, out curChunk);
                        }
                        if (!gotChunkRef)
                        {
                            writerEvent.Wait();
                        }
                        else
                        {
                            refChunkID = chunkID;
                        }
                    }
                }

                // write footer
                byte[] footer = new byte[] { // big endian crc32 and file size mod 2^32
                    (byte)_crc32,
                    (byte)(_crc32 >> 8),
                    (byte)(_crc32 >> 16),
                    (byte)(_crc32 >> 24),
                    (byte)_fileSize,
                    (byte)(_fileSize >> 8),
                    (byte)(_fileSize >> 16),
                    (byte)(_fileSize >> 24)
                };
                fs.Write(footer, 0, footer.Length);
            }
        }

        private void WorkerThread()
        {
            Block curBlock = null;
            List<Block> curChunk = null;
            uint curChunkID = uint.MaxValue;
            while (true)
            {
                _semFull.Wait();
                lock (_inputQueue)
                {
                    curBlock = _inputQueue.Dequeue();
                }
                _semEmpty.Release();

                // break if dummy block encountered
                if (curBlock.GetData == null)
                {
                    Console.WriteLine($"Received dummy block: worker {Thread.CurrentThread.Name}, finishing.");
                    break;
                }

                curBlock.processBlock();
                uint chunkID = (uint)Math.Floor(curBlock.ID / (double)_chunkSize);
                if (curChunkID != chunkID)
                {
                    curChunkID = chunkID;
                    lock (_outputDict)
                    {
                        if (!_outputDict.TryGetValue(chunkID, out curChunk))
                        {
                            // construct this chunk
                            curChunk = new List<Block>(new Block[_chunkSize]);
                            _outputDict.Add(chunkID, curChunk);
                        }
                    }
                }
                int id = (int)(curBlock.ID % _chunkSize);
                curChunk[id] = curBlock;
                curChunk[id].Finish();

                writerEvent.Set();
            }
        }

        public void Compress()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            Console.WriteLine($"Began compressing file: {_fileName}");
            Thread reader = new Thread(ReaderThread);
            reader.Name = "ReaderThread";
            reader.Start();

            Thread[] workers = new Thread[_nWorkers];
            for (int i = 0; i < _nWorkers; ++i)
            {
                workers[i] = new Thread(WorkerThread);
                workers[i].Name = $"{i}. worker";
                workers[i].Start();
            }

            Thread.CurrentThread.Name = "WriterThread";
            WriterThread();
            sw.Stop();
            Console.WriteLine($"Compression successfull. Time elapsed: {sw.ElapsedMilliseconds / 1000.0} s.");
        }

        static public void BeginDecompression(string fileName)
        {
            Console.WriteLine($"Began decompressing file: {fileName}");
            // remove gzip
            string outputFileName = Path.ChangeExtension(fileName, null);
            string orig_extension = Path.GetExtension(outputFileName);
            outputFileName = String.Concat(Path.ChangeExtension(outputFileName, null), "-decompressed", orig_extension);
            Console.WriteLine($"Output file will be: {outputFileName}");
            if (File.Exists(outputFileName))
            {
                File.Delete(outputFileName);
            }
            var fileInfo = new FileInfo(fileName);
            using (FileStream inputFS = fileInfo.OpenRead())
            {
                using (FileStream outputStream = File.Create(outputFileName))
                {
                    using (GZipStream decompressionStream = new GZipStream(inputFS, CompressionMode.Decompress))
                    {
                        decompressionStream.CopyTo(outputStream);
                    }
                }
            }
            Console.WriteLine("Decompression successfull.");
        }
    }
}
