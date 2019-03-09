package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/technix86/golang-tablescanner"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

var argBatchPath = flag.String("b", "", "[batch mode] Folder path for convert (all XLSX files are converted to CSV with same names by default)")
var argBatchPathFilenameMask = flag.String("bmask", "*/*.csv", "[batch mode] Output batch path mask like '*/converted/raw-*-out.csv')")
var argBatchThreads = flag.Int("bthreads", 1, "[batch mode] how many asynchronous workers should run, 0 for auto=numcpu")
var argXlsxPath = flag.String("f", "", "[single file mode] Path to input XLSX file")
var argCsvPath = flag.String("o", "", "[single file mode] Path to output CSV file (otherwise stdout)")
var argSheetIndex = flag.Int("i", -1, "[single file mode] Index of sheet to convert, zero based, -1=currently selected")
var argDelimiter = flag.String("d", ";", "Delimiter to use between fields")
var argFormatRaw = flag.Bool("raw", false, "Use real cell values instead of rendered with cell format")
var argFormatNoScientific = flag.Bool("nosci", true, "render scientific formats (4,60561E+12) as raw strings (4605610000000)")

func main() {
	flag.Parse()
	delimiterRune := []rune(*argDelimiter)[0]
	if len(*argXlsxPath) > 0 {
		err := xlsx2csv(*argXlsxPath, *argCsvPath, *argSheetIndex, delimiterRune, *argFormatRaw, *argFormatNoScientific)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	} else if len(*argBatchPath) > 0 {
		workerCount := *argBatchThreads
		if workerCount < 1 {
			workerCount = runtime.NumCPU() + 1
		}
		err := batchXlsx2csv(*argBatchPath, *argBatchPathFilenameMask, delimiterRune, workerCount, *argFormatRaw, *argFormatNoScientific)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	} else {
		flag.PrintDefaults()
		return
	}
}

func getRealCSVPath(destinationMask string, source string) string {
	source = filepath.ToSlash(source)
	destinationMask = filepath.ToSlash(destinationMask)
	dirname := filepath.Dir(source) + "/"
	basename := filepath.Base(source)
	ext := filepath.Ext(basename)
	basename = basename[0 : len(basename)-len(ext)]
	result := destinationMask
	result = strings.Replace(result, "*/", dirname, 1)
	result = strings.Replace(result, "*", basename, 1)
	return filepath.FromSlash(result)
}

type fileSortInfo struct {
	name string
	size int64
}

func batchXlsx2csv(batchPath string, batchPathFilenameMask string, delimiter rune, maxThreads int, formatRaw bool, formatFixSciNumbers bool) error {
	file, err := os.Open(batchPath)
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		return fmt.Errorf("%s is not a directory", batchPath)
	}
	dirContents, err := file.Readdir(0)
	if err != nil {
		return err
	}
	files := make([]fileSortInfo, 0) // file=>filesize
	for _, fileInner := range dirContents {
		fileSrc := file.Name() + string(os.PathSeparator) + fileInner.Name()
		ext := filepath.Ext(fileSrc)
		if !fileInner.IsDir() && strings.ToLower(ext) == ".xlsx" {
			files = append(files, fileSortInfo{name: fileSrc, size: fileInner.Size()})
		}
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].size > files[j].size
	})
	tasks := make(chan string, len(files))
	reports := make(chan int, len(files))
	for workerId := 0; workerId < maxThreads; workerId++ {
		go func(workerId int) {
			defer func() {
				fmt.Printf("STOP [%d]\n", workerId)
			}()
			for {
				fileSrcName, ok := <-tasks
				if !ok {
					return
				}
				fileDstName := getRealCSVPath(batchPathFilenameMask, fileSrcName)
				fmt.Printf("START[%d] %s\n", workerId, fileSrcName)
				err := xlsx2csv(fileSrcName, fileDstName, 0, delimiter, formatRaw, formatFixSciNumbers)
				if nil != err {
					fmt.Printf("  ERR[%d] %s: %s\n", workerId, fileSrcName, err.Error())
				}
				fmt.Printf("END  [%d] %s\n", workerId, fileDstName)
				reports <- 0
			}
		}(workerId)
	}
	for _, file := range files {
		tasks <- file.name
	}
	for range files {
		<-reports
	}
	close(tasks)
	time.Sleep(time.Millisecond * 100)
	return nil
}

func xlsx2csv(xlsxPath string, csvPath string, sheetIndex int, delimiter rune, formatRaw bool, formatFixSciNumbers bool) error {
	var scanner tablescanner.ITableDocumentScanner
	err, xlsx := tablescanner.NewXLSXStream(xlsxPath)
	if err != nil {
		return fmt.Errorf("cannot parse file [%s]: %s\n", xlsxPath, err.Error())
	}
	if formatRaw {
		xlsx.SetFormatRaw()
	} else if formatFixSciNumbers {
		xlsx.SetFormatFormattedSciFix()
	} else {
		xlsx.SetFormatFormatted()
	}
	scanner = xlsx
	defer xlsx.Close()
	var outputFile = os.Stdout
	var csvWriter *csv.Writer
	if "" != csvPath {
		outputFile, err = os.Create(csvPath)
		if nil != err {
			return fmt.Errorf("cannot crate file [%s]: %s\n", csvPath, err.Error())
		}
	}
	defer outputFile.Close()
	csvWriter = csv.NewWriter(outputFile)
	csvWriter.Comma = delimiter
	if "" == csvPath {
		fmt.Printf("GOT sheets=%#v\n", xlsx.GetSheets())
	}
	if sheetIndex >= 0 {
		err := xlsx.SetSheetId(sheetIndex)
		if nil != err {
			return err
		}
	}
	for nil == scanner.Scan() {
		data := scanner.GetScanned()
		csvWriter.Write(data)
	}
	csvWriter.Flush()
	outputFile.Close()
	return nil
}
