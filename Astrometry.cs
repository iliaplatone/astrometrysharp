using System;
using System.IO.Compression;
using System.IO;
using System.Diagnostics;
using System.Drawing;
using System.Collections.ObjectModel;
using System.Collections;
using System.Text;

namespace Astrometry
{
    public class ImageSolvedEventArgs : EventArgs
    {
        public Bitmap Image;
        public bool Success;
        public int JobID;
        public Hashtable WCSInfo;
        public ImageSolvedEventArgs(Bitmap image, bool success, Hashtable wcsinfo, int jobid)
        {
            Image = image;
            WCSInfo = wcsinfo;
            Success = success;
            JobID = jobid;
        }
    }
    public class LogDataEventArgs : EventArgs
    {
        public int Id;
        public string Data;
        public LogDataEventArgs(int id, string data)
        {
            Id = id;
            Data = data;
        }
    }
    public class Solver : IDisposable
    {
        string tmpdir = "";
        Collection<int> jobs = new Collection<int>();
        public event EventHandler<ImageSolvedEventArgs> ImageSolved;
        public event EventHandler<LogDataEventArgs> LogDataReceived;
        public Solver(string indexdir)
        {
            Instantiate(indexdir);
        }

        internal void Instantiate(string indexdir)
		{
			tmpdir = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData) + "/AstrometrySharp/";
			if (!IsLinux) {
				if (!Directory.Exists (tmpdir)) {
					Console.WriteLine ("Cannot load Astrometry extensions");
					return;
				}
				string drive = Path.GetPathRoot (tmpdir).Replace (@"\", "");
				indexdir = "\"/cygdrive/" + indexdir.Replace (@"\", "/").Replace (":", "") + "\"";
				File.WriteAllText (tmpdir + "/runme.bat",
				                   "@echo off\r\n" +
					drive + "\r\n" +
					"cd " + tmpdir + "\r\n" +
					"call runattrib\r\n" +
					"set PATH=/bin\r\n" +
					"set PYTHONHOME=/python\r\n" +
					"set PYTHONPATH=/python:/python/lib-dynload\r\n" +
					"bin\\bash solve %* " + indexdir + "\r\n"
				);
			} else {
				tmpdir = Path.GetTempPath() + "/";
			}
        }
        public int Solve(Bitmap image, int downsample, int depth, double app)
        {
            return Start(image, downsample, depth, app);
        }

        internal int Start(Bitmap image, int downsample, int depth, double app)
        {
            Process proc = new Process();
            string outfile = Path.GetFileNameWithoutExtension(Path.GetTempFileName());
            image.Save(tmpdir + outfile + ".png");
            if (!IsLinux)
            {
                proc.StartInfo.FileName = tmpdir + "runme.bat";
            }
            else
            {
                proc.StartInfo.FileName = "solve-field";
            }
			proc.StartInfo.Arguments = "--out " + (IsLinux ? "" : "/tmp/") + outfile + " --no-fits2fits --no-remove-lines --uniformize 0 --guess-scale --downsample " + downsample + " --depth " + depth + " --scale-units arcsecperpix --scale-low " + (app / 1.2) + " --scale-high " + (app * 1.2) + (IsLinux ? " " + tmpdir : " /") + outfile + ".png" + (!IsLinux ? " output" : "");
            proc.StartInfo.RedirectStandardOutput = true;
            proc.StartInfo.RedirectStandardInput = true;
            proc.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
            proc.StartInfo.CreateNoWindow = true;
            proc.StartInfo.UseShellExecute = false;
            proc.EnableRaisingEvents = true;
            proc.Exited += Astrometry_Exited;
            proc.OutputDataReceived += Astrometry_DataReceived;
            proc.ErrorDataReceived += Astrometry_DataReceived;
            proc.Start();
            proc.BeginOutputReadLine();
            jobs.Add(proc.Id);
            return proc.Id;
        }

        internal void Astrometry_DataReceived(object sender, DataReceivedEventArgs e)
        {
            try
            {
                Process proc = (Process)sender;
                if (e.Data != string.Empty)
                {
                    Console.WriteLine(e.Data);
                    if (LogDataReceived != null)
                        LogDataReceived(this, new LogDataEventArgs(proc.Id, e.Data));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message + Environment.NewLine + ex.StackTrace);
            }
        }

        internal void Astrometry_Exited(object sender, EventArgs e)
        {
            Process proc = (Process)sender;
            jobs.Remove(proc.Id);
            proc.Exited -= Astrometry_Exited;
            if (ImageSolved != null)
            {
                if (proc.ExitCode == 0)
                {
                    string[] args = proc.StartInfo.Arguments.Split(' ');
                    if (args.Length > 1)
                    {
                        string arg = tmpdir + Path.GetDirectoryName(args[1]) + "/" + Path.GetFileName(args[1]);
                        if (File.Exists(arg + "-ngc.png"))
                        {
                            Process p = new Process();
                            if (!IsLinux)
                            {
                                string drive = Path.GetPathRoot(tmpdir).Replace(@"\", "");
                                File.WriteAllText(tmpdir + "/info.bat",
                                "@echo off\n" +
                                drive + "\n" +
                                (tmpdir + "bin\\wcsinfo.exe").Replace("/", "\\") + " \"%1\" > \"%2\"\n"
                                );
                                p.StartInfo.FileName = tmpdir + "/info.bat";
                                p.StartInfo.Arguments = arg + ".wcs " + arg + ".txt";
                            }
                            else
                            {
                                p.StartInfo.FileName = "bash";
                                p.StartInfo.Arguments = "wcsinfo " + arg + ".wcs > " + arg + ".txt";
                            }
                            p.StartInfo.UseShellExecute = false;
                            p.StartInfo.CreateNoWindow = true;
                            p.Start();
                            p.WaitForExit();
                            Bitmap b = new Bitmap(arg + "-ngc.png");
                            ImageSolved(this, new ImageSolvedEventArgs((Bitmap)b.Clone(), true, fillWCS(File.ReadAllText(arg + ".txt")), proc.Id));
                            b.Dispose();
                        }
                        else
                            ImageSolved(this, new ImageSolvedEventArgs(null, false, null, proc.Id));
                    }
                    else
                        ImageSolved(this, new ImageSolvedEventArgs(null, false, null, proc.Id));
                }
            }
        }

        internal Hashtable fillWCS(string file)
        {
            Hashtable ret = new Hashtable();
            file = file.Replace("\n", " ");
            string[] scomposed = file.Split(' ');
            for (int i = 0; i < scomposed.Length - 2; i+=2)
            {
                ret.Add(scomposed[i], scomposed[i + 1]);
            }
            return ret;
        }

        public void Dispose()
        {
            try {
                foreach (int job in jobs)
                {
                    Process proc = Process.GetProcessById(job);
                    proc.Exited -= Astrometry_Exited;
                    proc.Close();
                    proc.Dispose();
                }
                ImageSolved = null;
            }
            catch { }
        }

        public static bool IsLinux
        {
            get
            {
                int p = (int)Environment.OSVersion.Platform;
                return (p == 4) || (p == 6) || (p == 128);
            }
        }
    }
}
