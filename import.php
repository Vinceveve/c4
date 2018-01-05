<?php
require_once 'vendor/autoload.php';

use Box\Spout\Reader\ReaderFactory;
use Box\Spout\Common\Type;
use League\CLImate\CLImate;
use Rx\Observable;
use Rx\Observer\CallbackObserver;

// TODO use env variable for config

$climate = new CLImate;
$climate->arguments->add([
    'file' => [
        'prefix' => 'f',
        'longPrefix' => 'file',
        'description' => 'Xls file to import',
        'defaultValue' => './storage/Portefeuille A.xlsx',
        'required' => true,
    ],
    'table' => [
        'prefix' => 't',
        'longPrefix' => 'table',
        'description' => 'name of table to insert to',
        'required' => true,
        'defaultValue' => 'portofolio',
    ],
    'help' => [
        'prefix' => 'h',
        'longPrefix' => 'help',
        'description' => 'Prints a usage statement',
        'noValue' => true,
    ],
]);
$climate->arguments->parse();

if ($climate->arguments->get('help')) {
    $help[] = 'Choose your usage :';
    $help[] = 'php import.php --file="./storage/Portefeuille A.xlsx" --table=portofolios';
    $help[] = 'php import.php --file="./storage/BDD CIA.xlsx" --table=companies';
    die(implode("\n", $help));
}
define('FILE', $climate->arguments->get('file'));
define('TABLE', $climate->arguments->get('table'));

$dotenv = new Dotenv\Dotenv(__DIR__);
$dotenv->load();


function to_column_name($str)
{
    $str = iconv(mb_detect_encoding($str), 'ASCII//IGNORE//TRANSLIT', $str);
    $str = strtolower($str);
    $str = str_replace(' ', '_', $str);
    return preg_replace("/[^A-Za-z0-9_]/", '', $str);
}


$reader = ReaderFactory::create(Type::XLSX);

$reader->open(FILE);

$reader->getSheetIterator()->rewind();
$sheet = $reader->getSheetIterator()->current();


$sheet->getRowIterator()->rewind();


// Start by building columns name
Observable::fromIterator($sheet->getRowIterator())
    // Take the first row
    ->take(1)
    // Each column will emit
    ->flatMap(['\Rx\Observable', 'fromArray'])
    // Filter empty column
    ->filter(\Rx\notEqualTo(""))
    // Get proper column name
    ->map('to_column_name')
    ->map(function ($col) {
        if ($col == 'factset_code') {
            $col = 'id';
        }
        return $col;
    })
    // Array with list of column
    ->toArray()
    ->subscribe(new CallbackObserver(function ($head) use ($sheet) {
        $total = 0;
        $postgres = new PgAsync\Client([
            "host" => $_ENV['DATABASE_HOST'],
            "port" => $_ENV['DATABASE_PORT'],
            "user" => $_ENV['DATABASE_USER'],
            "password" => $_ENV['DATABASE_PASSWORD'],
            "database" => $_ENV['DATABASE_NAME'],
            "auto_disconnect" => true //This option will force the client to disconnect as soon as it completes.  The connection will not be returned to the connection pool.

        ]);

        $sheet->getRowIterator()->rewind();
        Observable::fromIterator($sheet->getRowIterator())
            // Skip the header
            ->skip(1)
            // Map header and row only for non empty cells
            ->map(function ($row) use ($head, $postgres, &$total) {
                $insert = [];
                foreach ($row as $k => $v) {
                    if (empty($v)) continue;
                    if ($v instanceof DateTime) {
                        $v = $v->format("c");
                    }
                    $insert[$head[$k]] = str_replace("'", "''", $v);
                }
                $total++;
                return $insert;
            })
            ->flatMap(function ($row) use ($head, $postgres) {
                $keys = implode(',', array_keys($row));
                $values = implode("','", array_values($row));

                $query = "INSERT INTO ".TABLE."({$keys}) VALUES ('{$values}')";
                //echo $query."\n\n";
                return $postgres->query($query);
            })
            // Insert into mongodb
            ->subscribe(
                new CallbackObserver(
                    null,
                    function (\Exception $e) {
                        echo "Got an exception {$e->getMessage()}\n";
                    },
                    function () use (&$total) {
                        echo "\nData imported, {$total} rows inserted";
                    }
                )
            );
    }));


