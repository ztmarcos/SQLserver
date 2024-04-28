
import fs from 'fs';
import csv from 'csv-parser';
import sqlite3 from 'sqlite3';
import { format } from 'date-fns';

const { verbose } = sqlite3;

function createTableAndImportCSV(dbPath, csvFilePath) {
    console.log(`Database Path: ${dbPath}`);
    console.log(`CSV File Path: ${csvFilePath}`);

    return new Promise((resolve, reject) => {
        const db = new (verbose()).Database(dbPath, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE, (err) => {
            if (err) {
                console.error('Error opening database:', err.message);
                reject(err);
                return;
            }

            // Create or ensure main table exists
            const mainTableName = 'main_insurance_policies';
            const createMainTableSQL = `CREATE TABLE IF NOT EXISTS ${mainTableName} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Contratante TEXT,
                Número_de_póliza TEXT,
                Tipo_de_Póliza TEXT,
                Tipo_de_Plan TEXT,
                Dirección TEXT,
                R_F_C TEXT,
                Teléfono TEXT,
                Código_Cliente TEXT,
                Vigencia_Desde TEXT,
                Vigencia_Hasta TEXT,
                Fecha_de_Expedición TEXT,
                Forma_de_Pago TEXT,
                Prima_Neta_MXN REAL,
                Recargo_por_Pago_Fraccionado_MXN REAL,
                Importe_a_Pagar_MXN REAL,
                Beneficiarios TEXT,
                Edad_de_Contratación TEXT,
                Tipo_de_Riesgo TEXT,
                Fumador TEXT,
                Coberturas TEXT
            )`;

            db.run(createMainTableSQL, err => {
                if (err) {
                    console.error(`Error creating main table ${mainTableName}:`, err.message);
                    db.close();
                    reject(err);
                    return;
                }

                const tableName = `insurance_policies_${format(new Date(), 'yyyyMMddHHmmss')}`;
                console.log(`Creating new table: ${tableName}`);

                const createTableSQL = `CREATE TABLE IF NOT EXISTS ${tableName} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Contratante TEXT,
                    Número_de_póliza TEXT,
                    Tipo_de_Póliza TEXT,
                    Tipo_de_Plan TEXT,
                    Dirección TEXT,
                    R_F_C TEXT,
                    Teléfono TEXT,
                    Código_Cliente TEXT,
                    Vigencia_Desde TEXT,
                    Vigencia_Hasta TEXT,
                    Fecha_de_Expedición TEXT,
                    Forma_de_Pago TEXT,
                    Prima_Neta_MXN REAL,
                    Recargo_por_Pago_Fraccionado_MXN REAL,
                    Importe_a_Pagar_MXN REAL,
                    Beneficiarios TEXT,
                    Edad_de_Contratación TEXT,
                    Tipo_de_Riesgo TEXT,
                    Fumador TEXT,
                    Coberturas TEXT
                )`;

                db.run(createTableSQL, err => {
                    if (err) {
                        console.error(`Error creating table ${tableName}:`, err.message);
                        db.close();
                        reject(err);
                        return;
                    }

                    fs.createReadStream(csvFilePath)
                        .pipe(csv({
                            separator: ',',
                            mapHeaders: ({ header }) => header.trim().replace(/ /g, '_')
                        }))
                        .on('data', (row) => {
                            const insertSQL = `INSERT INTO ${tableName} (Contratante, Número_de_póliza, Tipo_de_Póliza, Tipo_de_Plan, Dirección, R_F_C, Teléfono, Código_Cliente, Vigencia_Desde, Vigencia_Hasta, Fecha_de_Expedición, Forma_de_Pago, Prima_Neta_MXN, Recargo_por_Pago_Fraccionado_MXN, Importe_a_Pagar_MXN, Beneficiarios, Edad_de_Contratación, Tipo_de_Riesgo, Fumador, Coberturas) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
                            const params = [
                                row.Contratante, row.Número_de_póliza, row.Tipo_de_Póliza, row.Tipo_de_Plan, row.Dirección, row.R_F_C,
                                row.Teléfono, row.Código_Cliente, row.Vigencia_Desde, row.Vigencia_Hasta, row.Fecha_de_Expedición, row.Forma_de_Pago,
                                row.Prima_Neta_MXN, row.Recargo_por_Pago_Fraccionado_MXN, row.Importe_a_Pagar_MXN, row.Beneficiarios,
                                row.Edad_de_Contratación, row.Tipo_de_Riesgo, row.Fumador, row.Coberturas
                            ];

                            db.run(insertSQL, params, (err) => {
                                if (err) {
                                    console.error('Error inserting data:', err.message);
                                } else {
                                    // Insert into main table as well
                                    const insertMainSQL = `INSERT INTO ${mainTableName} (Contratante, Número_de_póliza, Tipo_de_Póliza, Tipo_de_Plan, Dirección, R_F_C, Teléfono, Código_Cliente, Vigencia_Desde, Vigencia_Hasta, Fecha_de_Expedición, Forma_de_Pago, Prima_Neta_MXN, Recargo_por_Pago_Fraccionado_MXN, Importe_a_Pagar_MXN, Beneficiarios, Edad_de_Contratación, Tipo_de_Riesgo, Fumador, Coberturas) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
                                    db.run(insertMainSQL, params, err => {
                                        if (err) {
                                            console.error('Error inserting data into main table:', err.message);
                                        }
                                    });
                                }
                            });
                        })
                        .on('end', () => {
                            console.log(`CSV file processing complete. Data inserted into table ${tableName}.`);
                            db.close();
                            resolve(`New data successfully inserted into table ${tableName} and ${mainTableName}`);
                        })
                        .on('error', (err) => {
                            console.error('Error processing CSV file:', err.message);
                            db.close();
                            reject(err);
                        });
                });
            });
        });
    });
}

export { createTableAndImportCSV };
