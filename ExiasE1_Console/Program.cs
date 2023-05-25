using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Net.Sockets;
using System.Net;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Configuration;
using System.Globalization;

namespace ExiasE1_Console
{
    class Program
    {
        #region settings
        public static string IPadress = "172.18.95.31"; // cgm-app12, подсетка приборов
        public static int port = 8003;                  // порт

        public static string AnalyzerCode = "903";                // код из аналайзер конфигурейшн, который связывает прибор в PSMV2
        public static string AnalyzerConfigurationCode = "EXIAS"; // код прибора из аналайзер конфигурейшн

        public static string user = "PSMExchangeUser"; // логин для базы обмена файлами и для базы CGM Analytix
        public static string password = "PSM_123456";  // пароль для базы обмена файлами и для базы CGM Analytix   

        public static bool ServiceIsActive;        // флаг для запуска и остановки потока
        public static string AnalyzerResultPath = AppDomain.CurrentDomain.BaseDirectory + "\\AnalyzerResults"; // папка для файлов с результатами

        static object ExchangeLogLocker = new object();    // локер для логов обмена
        static object FileResultLogLocker = new object();  // локер для логов обмена
        static object ServiceLogLocker = new object();     // локер для логов драйвера
        #endregion

        #region Функции логов

        // лог обмена с анализатором
        static void ExchangeLog(string Message)
        {
            lock (ExchangeLogLocker)
            {
                string path = AppDomain.CurrentDomain.BaseDirectory + "\\Log\\Exchange";
                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }

                string filename = path + "\\ExchangeThread_" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";
                if (!System.IO.File.Exists(filename))
                {
                    using (StreamWriter sw = System.IO.File.CreateText(filename))
                    {
                        sw.WriteLine(DateTime.Now + ": " + Message);
                    }
                }
                else
                {
                    using (StreamWriter sw = System.IO.File.AppendText(filename))
                    {
                        sw.WriteLine(DateTime.Now + ": " + Message);
                    }
                }

            }
        }

        // Лог записи результатов в CGM
        static void FileResultLog(string Message)
        {
            try
            {
                lock (FileResultLogLocker)
                {
                    //string path = AppDomain.CurrentDomain.BaseDirectory + "\\Log\\FileResult" + "\\" + DateTime.Now.Year + "\\" + DateTime.Now.Month;
                    string path = AppDomain.CurrentDomain.BaseDirectory + "\\Log\\FileResult";
                    if (!Directory.Exists(path))
                    {
                        Directory.CreateDirectory(path);
                    }

                    //string filename = path + $"\\{FileName}" + ".txt";
                    string filename = path + $"\\ResultLog_" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";

                    if (!System.IO.File.Exists(filename))
                    {
                        using (StreamWriter sw = System.IO.File.CreateText(filename))
                        {
                            sw.WriteLine(DateTime.Now + ": " + Message);
                        }
                    }
                    else
                    {
                        using (StreamWriter sw = System.IO.File.AppendText(filename))
                        {
                            sw.WriteLine(DateTime.Now + ": " + Message);
                        }
                    }
                }
            }
            catch
            {

            }
        }

        // Лог драйвера
        static void ServiceLog(string Message)
        {
            lock (ServiceLogLocker)
            {
                try
                {
                    string path = AppDomain.CurrentDomain.BaseDirectory + "\\Log\\Service";
                    if (!Directory.Exists(path))
                    {
                        Directory.CreateDirectory(path);
                    }

                    string filename = path + "\\ServiceThread_" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";
                    if (!System.IO.File.Exists(filename))
                    {
                        using (StreamWriter sw = System.IO.File.CreateText(filename))
                        {
                            sw.WriteLine(DateTime.Now + ": " + Message);
                        }
                    }
                    else
                    {
                        using (StreamWriter sw = System.IO.File.AppendText(filename))
                        {
                            sw.WriteLine(DateTime.Now + ": " + Message);
                        }
                    }
                }
                catch
                {

                }
            }
        }

        #endregion

        #region функции
        //дописываем к номеру месяца ноль если нужно
        public static string CheckZero(int CheckPar)
        {
            string BackPar = "";
            if (CheckPar < 10)
            {
                BackPar = $"0{CheckPar}";
            }
            else
            {
                BackPar = $"{CheckPar}";
            }
            return BackPar;
        }

        // Создаем файл с результатом, отправленным анализатором
        static void MakeAnalyzerResultFile(string AllMessagePar)
        {
            if (!Directory.Exists(AnalyzerResultPath))
            {
                Directory.CreateDirectory(AnalyzerResultPath);
            }
            DateTime now = DateTime.Now;
            string filename = AnalyzerResultPath + "\\Results_" + now.Year + CheckZero(now.Month) + CheckZero(now.Day) + CheckZero(now.Hour) + CheckZero(now.Minute) + CheckZero(now.Second) + CheckZero(now.Millisecond) + ".res";
            using (System.IO.FileStream fs = new System.IO.FileStream(filename, FileMode.OpenOrCreate))
            {
                foreach (string res in AllMessagePar.Split('\r'))
                {
                    byte[] ResByte = Encoding.GetEncoding(1251).GetBytes(res + "\r\n");
                    fs.Write(ResByte, 0, ResByte.Length);
                }
            }
        }

        // Функция преобразования кода теста прибора в код теста PSMV2 в CGM
        public static string TranslateToPSMCodes(string AnalyzerTestCodesPar)
        {
            string BackTestCode = "";
            try
            {
                string CGMConnectionString = ConfigurationManager.ConnectionStrings["CGMConnection"].ConnectionString;
                //string CGMConnectionString = @"Data Source=CGM-APP11\SQLCGMAPP11;Initial Catalog=KDLPROD; Integrated Security=True;";
                CGMConnectionString = String.Concat(CGMConnectionString, $"User Id = {user}; Password = {password}");
                using (SqlConnection Connection = new SqlConnection(CGMConnectionString))
                {
                    Connection.Open();
                    // Ищем только тесты, которые настроены для прибора exias и настроены для PSMV2
                    SqlCommand TestCodeCommand = new SqlCommand(
                       "SELECT k1.amt_analyskod  FROM konvana k " +
                       "LEFT JOIN konvana k1 ON k1.met_kod = k.met_kod AND k1.ins_maskin = 'PSMV2' " +
                       $"WHERE k.ins_maskin = '{AnalyzerConfigurationCode}' AND k.amt_analyskod = '{AnalyzerTestCodesPar}' ", Connection);
                    SqlDataReader Reader = TestCodeCommand.ExecuteReader();

                    if (Reader.HasRows) // если есть данные
                    {
                        while (Reader.Read())
                        {
                            if (!Reader.IsDBNull(0)) { BackTestCode = Reader.GetString(0); };
                        }
                    }
                    Reader.Close();
                    Connection.Close();
                }
            }
            catch (Exception error)
            {
                FileResultLog($"Error: {error}");
            }

            return BackTestCode;
            /*
            switch (AnalyzerTestCodesPar)
            {
                case "K":
                    return "0025";
                case "Na":
                    return "0030";
                case "Cl":
                    return "0016";
                default:
                    return "";
            }
            */
        }

        #endregion

        #region Функция обработки файлов с результатами и создания файлов для службы, которая разберет файл и запишет данные в CGM
        static void ResultsProcessing()
        {
            while (ServiceIsActive)
            {
                try
                {
                    #region папки архива, результатов и ошибок
                    string OutFolder = ConfigurationManager.AppSettings["FolderOut"];
                    // архивная папка
                    string ArchivePath = AnalyzerResultPath + @"\Archive";
                    // папка для ошибок
                    string ErrorPath = AnalyzerResultPath + @"\Error";
                    // папка для файлов с результатами для CGM
                    //string CGMPath = AnalyzerResultPath + @"\CGM";

                    if (!Directory.Exists(ArchivePath))
                    {
                        Directory.CreateDirectory(ArchivePath);
                    }

                    if (!Directory.Exists(ErrorPath))
                    {
                        Directory.CreateDirectory(ErrorPath);
                    }

                    //if (!Directory.Exists(CGMPath))
                    //{
                     //   Directory.CreateDirectory(CGMPath);
                    //}
                    #endregion

                    // строки для формирования файла (psm файла) с результатами для службы,
                    // которая разбирает файлы и записывает результаты в CGM
                    string MessageHead = "";
                    string MessageTest = "";
                    string AllMessage = "";

                    // поолучаем список всех файлов в текущей папке
                    string[] Files = Directory.GetFiles(AnalyzerResultPath, "*.res");

                    // шаблоны регулярных выражений для поиска данных
                    string RIDPattern = @"[P][|][1][|]{2}(?<RID>\d+)[|]{1}\S*";
                    //string ResultPattern = @"\d+R[|]\d+[|](?<Test>\S+)[|]\S*";
                    string TestPattern = @"[R][|]\d+[|][@]+(?<Test>\w+)[@]\S*";
                    string ResultPattern = @"[R][|]\d+[|]\S+[|](?<Result>\d+[.]?\d*)[|]\S+[|]\S*";

                    Regex RIDRegex = new Regex(RIDPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                    Regex TestRegex = new Regex(TestPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                    Regex ResultRegex = new Regex(ResultPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));

                    // проходим по файлам
                    foreach (string file in Files)
                    {
                        Console.WriteLine(file);
                        FileResultLog(file);
                        string[] lines = System.IO.File.ReadAllLines(file);
                        string RID = "";
                        string Test = "";

                        // обрезаем только имя текущего файла
                        string FileName = file.Substring(AnalyzerResultPath.Length + 1);
                        // название файла .ок, который должен создаваться вместе с результирующим для обработки службой FileGetterService
                        string OkFileName = "";

                        // проходим по строкам в файле
                        foreach (string line in lines)
                        {
                            // заменяем птички на @, иначе регулярное врыажение некорректно работает
                            string line_ = line.Replace("^", "@");
                            Match RIDMatch = RIDRegex.Match(line_);
                            Match TestMatch = TestRegex.Match(line_);
                            Match ResultMatch = ResultRegex.Match(line_);

                            // поиск RID в строке
                            if (RIDMatch.Success)
                            {
                                RID = RIDMatch.Result("${RID}");
                                Console.WriteLine(RID);
                                FileResultLog($"Reguistion № {RID}");
                                MessageHead = $"O|1|{RID}||ALL|R|20230101000100|||||X||||ALL||||||||||F";
                            }

                            // поиск теста в строке
                            if (TestMatch.Success)
                            {
                                Test = TestMatch.Result("${Test}");
                                // преобразуем тест в код теста PSM
                                string PSMTestCode = TranslateToPSMCodes(Test);
                                Console.WriteLine(PSMTestCode);
                                //FileResultLog($"PSMV2 код: {PSMTestCode}");
                                string Result = "";
                                if (ResultMatch.Success)
                                {
                                    Result = ResultMatch.Result("${Result}");
                                    Console.WriteLine($"{Test} - result: {Result}");
                                    FileResultLog($"PSMV2 код: {PSMTestCode}");
                                    FileResultLog($"{Test} - result: {Result}");

                                    // нужно округлять значение результата до 2 цифр после запятой
                                    IFormatProvider formatter = new NumberFormatInfo { NumberDecimalSeparator = "." };
                                    double res = double.Parse(Result, formatter);
                                    //res = Convert.ToDouble(Result);
                                    res = Math.Round(res, 2);
                                    FileResultLog($"Результат округлен: {res}");
                                    Result = res.ToString();

                                    if ((PSMTestCode != "") && (Result != ""))
                                    {
                                        // формируем строку с ответом для результирующего файла
                                        MessageTest = MessageTest + $"R|1|^^^{PSMTestCode}^^^^{AnalyzerCode}|{Result}|||N||F||ExiasE1^||20230101000001|{AnalyzerCode}" + "\r";
                                        //Console.WriteLine(MessageTest);
                                    }
                                }

                                /*
                                // если код тест был интерпретирован и результат не пустой
                                //if (PSMTestCode != "")
                                if ((PSMTestCode != "") && (Result != ""))
                                {
                                    // формируем строку с ответом для результирующего файла
                                    MessageTest = MessageTest + $"R|1|^^^{PSMTestCode}^^^^{AnalyzerCode}|{Result}|||N||F||ExiasE1^||20230101000001|{AnalyzerCode}" + "\r";
                                    //Console.WriteLine(MessageTest);
                                }
                                */
                            }
                        }

                        // получаем название файла .ок на основании файла с результатом
                        if (FileName.IndexOf(".") != -1)
                        {
                            OkFileName = FileName.Split('.')[0] + ".ok";
                            //Console.WriteLine(OkFileName);
                        }

                        // если строки с результатами и с ШК не пустые, значит формируем результирующий файл
                        if (MessageHead != "" && MessageTest != "")
                        {
                            try
                            {
                                // собираем полное сообщение с результатом
                                AllMessage = MessageHead + "\r" + MessageTest;
                                //Console.WriteLine(AllMessage);

                                // создаем файл для записи результата в папке для рез-тов
                                //if (!File.Exists(CGMPath + @"\" + FileName))
                                if (!File.Exists(OutFolder + @"\" + FileName))
                                {
                                    //using (StreamWriter sw = File.CreateText(CGMPath + @"\" + FileName))
                                    using (StreamWriter sw = File.CreateText(OutFolder + @"\" + FileName))
                                    {
                                        foreach (string msg in AllMessage.Split('\r'))
                                        {
                                            sw.WriteLine(msg);
                                        }
                                    }
                                }
                                else
                                {
                                    //File.Delete(CGMPath + @"\" + FileName);
                                    //using (StreamWriter sw = File.CreateText(CGMPath + @"\" + FileName))
                                    File.Delete(OutFolder + @"\" + FileName);
                                    using (StreamWriter sw = File.CreateText(OutFolder + @"\" + FileName))
                                    {
                                        foreach (string msg in AllMessage.Split('\r'))
                                        {
                                            sw.WriteLine(msg);
                                        }
                                    }
                                }

                                // создаем .ok файл в папке для рез-тов
                                if (OkFileName != "")
                                {
                                    //if (!File.Exists(CGMPath + @"\" + OkFileName))
                                    if (!File.Exists(OutFolder + @"\" + OkFileName))
                                    {
                                        //using (StreamWriter sw = File.CreateText(CGMPath + @"\" + OkFileName)
                                        using (StreamWriter sw = File.CreateText(OutFolder + @"\" + OkFileName))
                                        {
                                            sw.WriteLine("ok");
                                        }
                                    }
                                    else
                                    {
                                        //File.Delete(CGMPath + OkFileName);
                                        //using (StreamWriter sw = File.CreateText(CGMPath + @"\" + OkFileName))
                                        File.Delete(OutFolder + OkFileName);
                                        using (StreamWriter sw = File.CreateText(OutFolder + @"\" + OkFileName))
                                        {
                                            sw.WriteLine("ok");
                                        }
                                    }
                                }

                                // помещение файла в архивную папку
                                if (File.Exists(ArchivePath + @"\" + FileName))
                                {
                                    File.Delete(ArchivePath + @"\" + FileName);
                                }
                                File.Move(file, ArchivePath + @"\" + FileName);

                                FileResultLog("Файл обработан и перемещен в папку Archive");
                                FileResultLog("");
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e);
                                FileResultLog(e.ToString());
                                // помещение файла в папку с ошибками
                                if (File.Exists(ErrorPath + @"\" + FileName))
                                {
                                    File.Delete(ErrorPath + @"\" + FileName);
                                }
                                File.Move(file, ErrorPath + @"\" + FileName);

                                FileResultLog("Ошибка обработки файла. Файл перемещен в папку Error");
                                FileResultLog("");
                            }
                        }
                        // или сюда блок else  и помещение файла в ошибки
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    FileResultLog(ex.ToString());
                }

                Thread.Sleep(1000);
            }

        }
        #endregion

        // TCP server
        static void TCPServer()
        {
            try
            {
                while (ServiceIsActive)
                {
                    IPAddress ip = IPAddress.Parse(IPadress);
                    // локальная точка EndPoint, на которой сокет будет принимать подключения от клиентов
                    EndPoint endpoint = new IPEndPoint(ip, port);
                    // создаем сокет
                    Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                    // связываем сокет с локальной точкой endpoint 
                    socket.Bind(endpoint);

                    // получаем конечную точку, с которой связан сокет
                    Console.WriteLine(socket.LocalEndPoint);
                    ServiceLog(socket.LocalEndPoint.ToString());

                    // запуск прослушивания подключений
                    socket.Listen(1000);
                    Console.WriteLine("TCP Сервер запущен. Ожидание подключений...");
                    ServiceLog("TCP Сервер запущен. Ожидание подключений...");
                    // После начала прослушивания сокет готов принимать подключения
                    // получаем входящее подключение
                    Socket client = socket.Accept();

                    // получаем адрес клиента, который подключился к нашему tcp серверу
                    Console.WriteLine($"Адрес подключенного клиента: {client.RemoteEndPoint}");
                    ServiceLog($"Адрес подключенного клиента: {client.RemoteEndPoint}");

                    int ServerCount = 0; // счетчик

                    while (ServiceIsActive)
                    {
                        // состояние сокета
                        // client.Poll(1, SelectMode.SelectRead) - true, если:
                        // если был вызван метод Listen(Int32) и подключение отложено
                        // если данные доступны для чтения
                        // если подключение закрыто, сброшено или завершено
                        // Console.WriteLine($"handler.Available {client.Available}; " +
                        //   $"SelectRead: {client.Poll(1, SelectMode.SelectRead)};" +
                        //   $"SelectWrite: {client.Poll(1, SelectMode.SelectWrite)};" +
                        //   $"SelectError: {client.Poll(1, SelectMode.SelectError)};");

                        // нет данных для чтения и соединение не активно
                        if (client.Poll(1, SelectMode.SelectRead) && client.Available == 0)
                        {
                            //CloseConnectionForcely = false;
                            client = socket.Accept();
                            ServiceLog("Ожидание переподключения");
                        }

                        // если клиент ничего не посылает
                        if (client.Available == 0)
                        {
                            ServerCount++;
                            if (ServerCount == 100)
                            {
                                ServerCount = 0;

                                ServiceLog("Состояние сокета: " + 
                                           $"handler.Available {client.Available}; " +
                                           $"SelectRead: {client.Poll(1, SelectMode.SelectRead)}; " +
                                           $"SelectWrite: {client.Poll(1, SelectMode.SelectWrite)}; " +
                                           $"SelectError: {client.Poll(1, SelectMode.SelectError)};");
                                ServiceLog("Прослушивание сокета...");
                                ServiceLog("");
                            }
                        }
                        // есть данные на сокете, получаем сообщение от анализатора
                        else
                        {
                            //client.Send(ACK);
                            //Console.WriteLine("ACK");

                            // UTF8 encoder
                            Encoding utf8 = Encoding.UTF8;
                            // количество полученных байтов
                            int received_bytes = 0;
                            // буфер для получения данных
                            byte[] received_data = new byte[1024];
                            // StringBuilder для склеивания полученных данных в одну строку
                            var messageFromElite = new StringBuilder();

                            // состояние сокета
                            ServiceLog("Состояние сокета: " + 
                                           $"handler.Available {client.Available}; " +
                                           $"SelectRead: {client.Poll(1, SelectMode.SelectRead)}; " +
                                           $"SelectWrite: {client.Poll(1, SelectMode.SelectWrite)}; " +
                                           $"SelectError: {client.Poll(1, SelectMode.SelectError)};");
                            ServiceLog("Есть данные на сокете. Получение сообщения от анализатора.");
                            ServiceLog("");

                            do
                            {
                                received_bytes = client.Receive(received_data);
                                // GetString - декодирует последовательность байтов из указанного массива байтов в строку.
                                // преобразуем полученный набор байтов в строку
                                string ResponseMsg = Encoding.UTF8.GetString(received_data, 0, received_bytes);
                                //messageFromElite.Append(Encoding.UTF8.GetString(received_data, 0, received_bytes));

                                // добавляем в StringBuilder
                                messageFromElite.Append(ResponseMsg);
                                ExchangeLog(messageFromElite.ToString());
                            }
                            while (client.Available > 0);

                            // нужно заменить птички, иначе рег.выражение не работает
                            // string messageElite = messageFromElite.ToString().Replace("^", "@");

                            // MakeAnalyzerResultFile(messageElite);
                            MakeAnalyzerResultFile(messageFromElite.ToString());
                        }
                        Thread.Sleep(1000);
                    }

                    //Thread.Sleep(1000);
                }
            }
            catch (Exception error)
            {
                ServiceLog($"Exception: {error}");
                Console.WriteLine($"Exception: {error}");
            }
        }


        static void Main(string[] args)
        {
            ServiceIsActive = true;
            Console.WriteLine("Сервис начал работу.");
            ServiceLog("Сервис начал работу.");

            //TCP сервер для прибора
            Thread TCPServerThread = new Thread(new ThreadStart(TCPServer));
            TCPServerThread.Name = "TCPServer";
            TCPServerThread.Start();

            //ResultsProcessing();

            // Поток обработки результатов
            Thread ResultProcessingThread = new Thread(ResultsProcessing);
            ResultProcessingThread.Name = "ResultsProcessing";
            ResultProcessingThread.Start();
            
            Console.ReadLine();
        }
    }
}
