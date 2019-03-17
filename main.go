package main

// @todo: arg help about setDateFixedFormat
// @todo: add i18n arg

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/technix86/golang-tablescanner"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

type TRunParameters struct {
	XLSXPath                *string
	CSVPath                 *string
	SheetIndex              *int
	BatchPath               *string
	BatchPathFilenameMask   *string
	BatchThreads            *int
	Delimiter               *string
	FormatRaw               *bool
	FormatI18n              *string
	FormatAllowExpFmt       *bool
	FormatDecimalSeparator  *string
	FormatThousandSeparator *string
	FormatDateFixed         *string
	AddBOMUTF8              *bool
	AutoTrim                *bool
}

const dummyThousandSeparator = "depends on i18n"

var runParameters = &TRunParameters{
	XLSXPath:                flag.String("xlsx", "", "[single file mode] Path to input XLSX/XLS file"),
	CSVPath:                 flag.String("csv", "", "[single file mode] Path to output CSV file (stdout of empty)"),
	BatchPath:               flag.String("batch", "", "[batch mode] Folder path for convert (all .xlsx/.xls files are converted to CSV with same names by default)"),
	BatchPathFilenameMask:   flag.String("batchMask", "*/*.csv", "[batch mode] Output batch path mask like '*/converted/raw-*-out.csv')"),
	BatchThreads:            flag.Int("batchThreads", 1, "[batch mode] how many asynchronous workers should run, 0 for auto=numcpu"),
	SheetIndex:              flag.Int("sheet", -1, "Index of sheet to convert, zero based, -1=currently selected"),
	Delimiter:               flag.String("delimiter", ";", "CSV delimiter"),
	FormatRaw:               flag.Bool("fmtRaw", false, "[XLSX only] Use real cell values instead of rendered with cell format"),
	FormatI18n:              flag.String("fmtI18n", "en", "[XLSX only] Use specific I18n for builtin number formats"),
	FormatAllowExpFmt:       flag.Bool("fmtAllowExp", false, "[XLSX only] Render scientific formats 4,60561E+12 as raw strings 4605610000000"),
	FormatDecimalSeparator:  flag.String("fmtDecimal", "", "[XLSX only] Custom decimal separator for number formats"),
	FormatThousandSeparator: flag.String("fmtThousand", dummyThousandSeparator, "[XLSX only] Custom thousand separator for number formats"),
	FormatDateFixed:         flag.String("fmtDateFixed", "", "[XLSX only] Custom date format for any datetime cell"),
	AddBOMUTF8:              flag.Bool("bom", false, "Start output stream/file/files with UTF-8 BOM = EF BB BF"),
	AutoTrim:                flag.Bool("trim", false, "Trim whitespaces"),
}

func main() {
	flag.Parse()
	if len(*runParameters.XLSXPath) > 0 {
		err := xlsx2csv(runParameters)
		if err != nil {
			_, _ = os.Stderr.WriteString(fmt.Sprintf("main.XLSX2CSV() error: %s\n", err.Error()))
		}
	} else if len(*runParameters.BatchPath) > 0 {
		if *runParameters.BatchThreads < 1 {
			*runParameters.BatchThreads = runtime.NumCPU() + 1
		}
		err := batchXlsx2csv(runParameters)
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

func batchXlsx2csv(runParameters *TRunParameters) error {
	file, err := os.Open(*runParameters.BatchPath)
	if err != nil {
		return err
	}
	defer nowarnCloseCloser(file)
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		return fmt.Errorf("%s is not a directory", *runParameters.BatchPath)
	}
	dirContents, err := file.Readdir(0)
	if err != nil {
		return err
	}
	files := make([]fileSortInfo, 0) // file=>filesize
	for _, fileInner := range dirContents {
		if fileInner.IsDir() {
			continue
		}
		fileSrc := file.Name() + string(os.PathSeparator) + fileInner.Name()
		ext := filepath.Ext(fileSrc)
		if strings.ToLower(ext) == ".xlsx" || strings.ToLower(ext) == ".xls" {
			files = append(files, fileSortInfo{name: fileSrc, size: fileInner.Size()})
		}
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].size > files[j].size
	})
	tasks := make(chan string, len(files))
	reports := make(chan int, len(files))
	for workerId := 0; workerId < *runParameters.BatchThreads; workerId++ {
		go func(workerId int) {
			defer func() {
				fmt.Printf("STOP [%d]\n", workerId)
			}()
			for {
				fileSrcName, ok := <-tasks
				if !ok {
					return
				}
				fileDstName := getRealCSVPath(*runParameters.BatchPathFilenameMask, fileSrcName)
				runThreadParameters := *runParameters
				runThreadParameters.XLSXPath = &fileSrcName
				runThreadParameters.CSVPath = &fileDstName
				fmt.Printf("START[%d] %s\n", workerId, fileSrcName)
				err := xlsx2csv(&runThreadParameters)
				if nil != err {
					_, _ = os.Stderr.WriteString(fmt.Sprintf("  ERR[%d] %s: %s\n", workerId, fileSrcName, err.Error()))
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

func xlsx2csv(runParameters *TRunParameters) error {
	var scanner tablescanner.ITableDocumentScanner
	err, xlsx := tablescanner.NewTableStream(*runParameters.XLSXPath)

	if err != nil {
		return fmt.Errorf("cannot parse file [%s]: %s\n", *runParameters.XLSXPath, err.Error())
	}
	_ = xlsx.SetI18n(*runParameters.FormatI18n) // just try if possible
	xlsx.Formatter().SetDateFixedFormat(*runParameters.FormatDateFixed)
	if *runParameters.FormatThousandSeparator != dummyThousandSeparator {
		xlsx.Formatter().SetThousandSeparator(*runParameters.FormatThousandSeparator)
	}
	xlsx.Formatter().SetDecimalSeparator(*runParameters.FormatDecimalSeparator)
	if *runParameters.AutoTrim {
		xlsx.Formatter().SetTrimOn()
	} else {
		xlsx.Formatter().SetTrimOff()
	}
	if *runParameters.FormatRaw {
		xlsx.Formatter().DisableFormatting()
	} else {
		xlsx.Formatter().EnableFormatting()
		if *runParameters.FormatAllowExpFmt {
			xlsx.Formatter().AllowScientific()
		} else {
			xlsx.Formatter().DenyScientific()
		}
	}
	scanner = xlsx
	defer nowarnCloseCloser(xlsx)
	var outputFile = os.Stdout
	var csvWriter *csv.Writer
	if "" != *runParameters.CSVPath {
		err = os.MkdirAll(filepath.Dir(*runParameters.CSVPath), 0775)
		if nil != err {
			return err
		}
		outputFile, err = os.Create(*runParameters.CSVPath)
		if nil != err {
			return fmt.Errorf("cannot create file [%s]: %s\n", *runParameters.CSVPath, err.Error())
		}
	}
	defer nowarnCloseCloser(outputFile)
	csvWriter = csv.NewWriter(outputFile)
	defer csvWriter.Flush()
	csvWriter.Comma = []rune(*runParameters.Delimiter)[0]
	if *runParameters.SheetIndex >= 0 {
		err := xlsx.SetSheetId(*runParameters.SheetIndex)
		if nil != err {
			return err
		}
	}
	iteration := 0
	for nil == scanner.Scan() {
		if *runParameters.AddBOMUTF8 && iteration == 0 {
			_, err = outputFile.Write([]byte{0xEF, 0xBB, 0xBF})
			if nil != err {
				return err
			}
		}
		data := scanner.GetScanned()
		err := csvWriter.Write(data)
		if nil != err {
			return err
		}
		iteration++
		if iteration%10000 == 0 {
			csvWriter.Flush()
		}
	}
	returnError := scanner.GetLastScanError()
	if returnError == io.EOF {
		returnError = nil
	}
	return returnError
}

func nowarnCloseCloser(rc io.Closer) {
	_ = rc.Close()
}
