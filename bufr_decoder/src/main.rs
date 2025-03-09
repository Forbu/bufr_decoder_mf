use std::fs::File;
use std::io::{self, Read, BufReader};
use std::error::Error;
use std::collections::HashMap;
use csv::ReaderBuilder;
use std::path::Path;

struct BitReader<R: Read> {
    input: R,
    accumulator: u8,
    bcount: u8,
    read: usize,
    total_read: usize,
}

impl<R: Read> BitReader<R> {
    pub fn new(input: R) -> Self {
        BitReader {
            input,
            accumulator: 0,
            bcount: 0,
            read: 0,
            total_read: 0,
        }
    }

    fn _readbit(&mut self) -> Result<u8, Box<dyn Error>> {
        if self.bcount == 0 {
            let mut buffer = [0];
            let bytes_read = self.input.read(&mut buffer)?;
            if bytes_read == 0 {
                return Err(From::from("End of file reached"));
            }
            self.accumulator = buffer[0];
            self.bcount = 8;
            self.read = bytes_read;
        }
        let rv = (self.accumulator & (1 << (self.bcount - 1))) >> (self.bcount - 1);
        self.bcount -= 1;
        Ok(rv)
    }

    pub fn read_bits(&mut self, n: u32) -> Result<u32, Box<dyn Error>> {
        self.total_read += 1;
        let mut v: u32 = 0;
        for _ in 0..n {
            v = (v << 1) | (self._readbit()? as u32);
        }
        Ok(v)
    }
}

fn bits_to_bytes(chaine: u32) -> Result<String, Box<dyn Error>> {
    let byte_number = chaine.leading_zeros() as u32 / 8;
    let bin_array = chaine.to_be_bytes();
    let bin_array_trimmed = &bin_array[byte_number as usize..];
    let result = String::from_utf8(bin_array_trimmed.to_vec())?;
    Ok(result)
}

fn bytes_desc(byt: u8) -> String {
    if byt < 64 {
        format!("0-{}-", byt)
    } else if byt < 128 {
        format!("1-{}-", byt - 64)
    } else if byt < 192 {
        format!("2-{}-", byt - 128)
    } else {
        format!("3-{}-", byt - 192)
    }
}

// Define structs to hold table data, replacing pandas DataFrames
#[derive(Debug)]
struct TableBRecord {
    f: String,
    x: String,
    y: String,
    description: String,
    unit: String,
    scale: String,
    reference_value: String,
    data_width_bits: String,
}

#[derive(Debug)]
struct TableDRecord {
    f: String,
    x: String,
    y: String,
    df: String,
    dx: String,
    dy: String,
}


fn tables_b(file_path: &str) -> Result<Vec<TableBRecord>, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut rdr = ReaderBuilder::new().delimiter(b';').has_headers(false).from_reader(reader);
    let mut records = Vec::new();
    for result in rdr.records() {
        let record = result?;
        if record.len() == 8 {
            records.push(TableBRecord {
                f: record[0].to_string(),
                x: record[1].to_string(),
                y: record[2].to_string(),
                description: record[3].to_string(),
                unit: record[4].to_string(),
                scale: record[5].to_string(),
                reference_value: record[6].to_string(),
                data_width_bits: record[7].to_string(),
            });
        }
    }
    Ok(records)
}

fn tables_d(file_path: &str) -> Result<Vec<TableDRecord>, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut rdr = ReaderBuilder::new().delimiter(b';').has_headers(false).from_reader(reader);
    let mut records = Vec::new();
    for result in rdr.records() {
        let record = result?;
         if record.len() == 6 {
            records.push(TableDRecord {
                f: record[0].to_string(),
                x: record[1].to_string(),
                y: record[2].to_string(),
                df: record[3].to_string(),
                dx: record[4].to_string(),
                dy: record[5].to_string(),
            });
        }
    }
    Ok(records)
}


fn dico_descriptor_b(table_b_records: Vec<TableBRecord>) -> Result<HashMap<String, HashMap<String, String>>, Box<dyn Error>> {
    let mut dico_desc: HashMap<String, HashMap<String, String>> = HashMap::new();
    for record in table_b_records {
        let key = format!("{}-{}-{}", record.f, record.x, record.y);
        let mut value_map: HashMap<String, String> = HashMap::new();
        value_map.insert("Description".to_string(), record.description.clone());
        value_map.insert("Unit".to_string(), record.unit.clone());
        value_map.insert("Scale".to_string(), record.scale.clone());
        value_map.insert("Ref_Val".to_string(), record.reference_value.clone());
        value_map.insert("Data_width_bits".to_string(), record.data_width_bits.clone());
        dico_desc.insert(key, value_map);
    }
    Ok(dico_desc)
}


fn dico_descriptor_d(table_d_records: Vec<TableDRecord>) -> Result<HashMap<String, Vec<String>>, Box<dyn Error>> {
    let mut dico_desc: HashMap<String, Vec<String>> = HashMap::new();
    let mut listed: Vec<String> = Vec::new();
    let mut key1 = String::new();

    for record in table_d_records {
        if !record.f.is_empty() && record.f.ends_with("3") {
             dico_desc.insert(key1.clone(), listed.clone());
             listed = Vec::new();
             key1 = format!("{}-{}-{}", record.f, record.x, record.y);
             listed.push(format!("{}-{}-{}", record.df, record.dx, record.dy));
        } else if !record.df.is_empty() {
             listed.push(format!("{}-{}-{}", record.df, record.dx, record.dy));
        }
    }
    dico_desc.insert(key1.clone(), listed.clone());
    Ok(dico_desc)
}

struct BufrDecoder {
    dir_path_table: String,
    fic_tab_b: String,
    fic_tab_d: String,
    fic_local_tab_b: String,
    fic_local_tab_d: String,
    affiche_descriptors: bool,
    dico_m_b: HashMap<String, HashMap<String, String>>,
    dico_m_d: HashMap<String, Vec<String>>,
    dico_l_b: HashMap<String, HashMap<String, String>>,
    dico_l_d: HashMap<String, Vec<String>>,
    datas_total: HashMap<String, Vec<f64>>, // Store decoded data
    datas_unites: HashMap<String, String>,
    bit_width_plus: u32,
    bit_scale_plus: i32,
    bit_ref_changed: bool,
    bit_new_ref: HashMap<String, f64>,
    bit_new_width: u32,
}

impl BufrDecoder {
    pub fn new(dir_path_table: String, fic_tab_b: String, fic_tab_d: String, fic_local_tab_b: String, fic_local_tab_d: String, affiche_descriptors: bool) -> Self {
        BufrDecoder {
            dir_path_table,
            fic_tab_b,
            fic_tab_d,
            fic_local_tab_b,
            fic_local_tab_d,
            affiche_descriptors,
            dico_m_b: HashMap::new(),
            dico_m_d: HashMap::new(),
            dico_l_b: HashMap::new(),
            dico_l_d: HashMap::new(),
            datas_total: HashMap::new(), // Initialize data storage
            datas_unites: HashMap::new(),
            bit_width_plus: 0,
            bit_scale_plus: 0,
            bit_ref_changed: false,
            bit_new_ref: HashMap::new(),
            bit_new_width: 0,
        }
    }

    fn descri(&self, desc: &str) -> Option<&HashMap<String, String>> {
        if let Some(r) = self.dico_l_b.get(desc) {
            if self.affiche_descriptors {
                println!("{} : {:?}", desc, r);
            }
            return Some(r);
        } else if let Some(r) = self.dico_l_d.get(desc) {
            if self.affiche_descriptors {
                println!("{} : {:?}", desc, r);
            }
            return None; // D table returns Vec<String>, not HashMap, so return None here and handle D table lookups differently if needed
        } else if let Some(r) = self.dico_m_b.get(desc) {
            if self.affiche_descriptors {
                println!("{} : {:?}", desc, r);
            }
            return Some(r);
        } else if let Some(r) = self.dico_m_d.get(desc) {
            if self.affiche_descriptors {
                println!("{} : {:?}", desc, r);
            }
            return None; // Same as above for master D table
        } else {
            if self.affiche_descriptors {
                println!("{} UNKNOWN", desc);
            }
            return None;
        }
    }

    fn simple_desc<R: Read>(&mut self, desc_elt: &str, reader: &mut BitReader<R>) -> Result<(), Box<dyn Error>> {
        if let Some(descript_elt) = self.descri(desc_elt) {
            let mut longueur: u32 = descript_elt.get("Data_width_bits").unwrap().parse::<u32>().unwrap_or(0); // Get data width from descriptor
            if self.bit_new_width != 0 {
                longueur = self.bit_new_width;
            }
            longueur += self.bit_width_plus;


            let description = descript_elt.get("Description").unwrap_or(&String::from("No Description")).clone();
            if self.affiche_descriptors {
                println!("longueur : {}, Description : {}", longueur, description);
            }

            let tot_bits = reader.read_bits(longueur)?;
            let scale: f64 = descript_elt.get("Scale").unwrap_or(&String::from("0")).parse::<f64>().unwrap_or(0.0) + self.bit_scale_plus as f64;
            let mut ref_val: f64 = descript_elt.get("Ref_Val").unwrap_or(&String::from("0")).parse::<f64>().unwrap_or(0.0);

            if self.bit_ref_changed && self.bit_new_ref.contains_key(desc_elt) {
                ref_val += self.bit_new_ref.get(desc_elt).unwrap_or(&0.0);
            }


            let val_data = (tot_bits as f64 + ref_val) / 10f64.powf(scale);

            let unit = descript_elt.get("Unit").unwrap_or(&String::from("")).clone();
            if unit == "CCITT IA5" {
                match bits_to_bytes(tot_bits) {
                    Ok(byte_str) => println!("  \"{}\"", byte_str),
                    Err(_) => println!("  (Non-printable CCITT IA5 data)"),
                }
            } else if self.affiche_descriptors {
                println!("  = {} {}", val_data, unit);
            }

            self.datas_total.entry(description.clone()).or_insert_with(Vec::new).push(val_data);
            self.datas_unites.entry(description).or_insert(unit);
        }
        Ok(())
    }

    fn descri_table_c<R: Read>(&mut self, reader: &mut BitReader<R>, descriptor: &str, descriptors: &mut Vec<String>, index_descript: &mut usize) -> Result<(), Box<dyn Error>> {
        let new_ref = descriptor.split('-').nth(2).unwrap_or("0").parse::<i32>().unwrap_or(0);
        match descriptor.split('-').nth(1).unwrap_or("0").parse::<i32>().unwrap_or(0) {
            1 => { // change data width
                self.bit_width_plus = if new_ref == 0 { 0 } else { (new_ref - 128) as u32 };
            },
            2 => { // change scale
                self.bit_scale_plus = if new_ref == 0 { 0 } else { new_ref - 128 };
            },
            3 => { // change reference value
                if new_ref > 0 {
                    self.bit_ref_changed = true;
                    let ybits = descriptor.split('-').nth(2).unwrap_or("0").parse::<u32>().unwrap_or(0);
                    *index_descript += 1;
                    while *index_descript < descriptors.len() && descriptors[*index_descript] != "2-3-255" {
                        let desc_new = &descriptors[*index_descript];
                        let result = reader.read_bits(ybits)?;
                        let ref_val = if result >= 2u32.pow(ybits - 1) {
                            -1.0 * (result - 2u32.pow(ybits - 1)) as f64
                        } else {
                            result as f64
                        };
                        self.bit_new_ref.insert(desc_new.clone(), ref_val);
                        *index_descript += 1;
                    }
                    if *index_descript < descriptors.len() && descriptors[*index_descript] == "2-3-255" {
                        // consume "2-3-255"
                    }
                } else {
                    self.bit_ref_changed = false;
                    self.bit_new_ref.clear();
                }
                 *index_descript -= 1; // to compensate for the increment in main loop
            },
            8 => { // change bit width
                self.bit_new_width = if new_ref == 0 { 0 } else { 8 * new_ref as u32 };
            },
            _ => {}
        }
        Ok(())
    }


    fn decode_bufr_message<R: Read>(&mut self, reader: &mut BitReader<R>, bytes_size: u32) -> Result<Option<HashMap<String, Vec<f64>>>, Box<dyn Error>> {
        println!(" ----------- BEGIN OF BUFR MESSAGE -----------");
        self.datas_total.clear(); // Clear data for each message
        self.datas_unites.clear();
        self.bit_width_plus = 0;
        self.bit_scale_plus = 0;
        self.bit_ref_changed = false;
        self.bit_new_ref.clear();
        self.bit_new_width = 0;


        let x = reader.read_bits(4 * bytes_size)?;
         if x != 0x42554652 { // BUFR magic number
            return Ok(None);
        }
        println!("Entete: BUFR");

        let total_length = reader.read_bits(3 * bytes_size)?;
        println!("Total length of Bufr message in bytes : {}", total_length);
        let edition_number = reader.read_bits(1 * bytes_size)?;
        println!("Bufr Edition number : {}", edition_number);


        // SECTION 1
        let version = edition_number; // Assuming edition number is version for now
        let (length_1, master_table_number, sub_center_id, center_id, sect2) =
            if version == 2 {
                self.section1_v2(reader, bytes_size)?
            } else if version == 4 {
                self.section1_v4(reader, bytes_size)?
            } else {
                println!("Version Inconnue");
                return Ok(None);
            };

        let master_table_version = reader.read_bits(1 * bytes_size)?;
        println!("Version number of master table used : {}", master_table_version);
        let local_table_version = reader.read_bits(1 * bytes_size)?;
        println!("Version number of local tables used : {}", local_table_version);

         // LOAD TABLES - This should be done only once in the decoder init for efficiency, unless tables can change mid-file.
        self.load_tables(master_table_version, center_id, local_table_version)?;


        let year = if version == 2 {
            reader.read_bits(1 * bytes_size)?
        } else { // version == 4
            reader.read_bits(2 * bytes_size)?
        };
        println!("Year : {}", year);
        let month = reader.read_bits(1 * bytes_size)?;
        println!("Month : {}", month);
        let day = reader.read_bits(1 * bytes_size)?;
        println!("Day : {}", day);
        let hour = reader.read_bits(1 * bytes_size)?;
        println!("Hour : {}", hour);
        let minute = reader.read_bits(1 * bytes_size)?;
        println!("Minute : {}", minute);
        if version == 4 {
            let second = reader.read_bits(1 * bytes_size)?;
            println!("Second : {}", second);
        }

        self.section1end(version, length_1, reader, bytes_size)?;

        if sect2 {
            self.section2(reader, bytes_size)?;
        }

         // SECTION 3 ( Data Description )
        let length_3 = reader.read_bits(3 * bytes_size)?;
        println!("Length of section 3 (Data Description) : {}", length_3);
        reader.read_bits(1 * bytes_size)?; // Reserved, set to 0
        let number_of_data_subsets = reader.read_bits(2 * bytes_size)?;
        println!("Number of data subsets : {}", number_of_data_subsets);
        let _observed_compressed_data = reader.read_bits(1 * bytes_size)?;
        // println!("Observed/Compressed Data : ", x//128 , '/', (x//64)%2);


        let mut descriptors: Vec<String> = Vec::new();
        let mut desc_bytes = String::new();

        for i in 0..(length_3 - 7) {
            let x = reader.read_bits(1 * bytes_size)?;
            if i % 2 == 1 {
                desc_bytes.push_str(&format!("{}", x));
                descriptors.push(format!("{}{}", bytes_desc(desc_bytes.as_bytes()[0]), desc_bytes.as_bytes()[1]));
                desc_bytes.clear();
            } else {
                desc_bytes = bytes_desc(x as u8);
            }
        }

        if self.affiche_descriptors {
            println!("Descriptors : {:?}", descriptors);
        }


        // SECTION 4 ( Datas )
        let length_4 = reader.read_bits(3 * bytes_size)?;
        println!("Length of section 4 (Datas) : {}", length_4);
        reader.read_bits(1 * bytes_size)?; // Reserved, SET TO 0

        let mut index_descript = 0;
        while index_descript < descriptors.len() {
            let descriptor = &descriptors[index_descript];
            if self.affiche_descriptors {
                println!("{}", descriptor);
            }

            if descriptor.starts_with("0-") {
                // F = 0 : single element descriptor (ref in Table B)
                self.simple_desc(descriptor, reader)?;
            } else if descriptor.starts_with("3-") {
                // F = 3 : list of descriptors (ref in table D)
                if let Some(descript_elt) = self.dico_m_d.get(descriptor) { // Master table D lookup
                    for eltk in descript_elt {
                        if self.affiche_descriptors {
                            println!("{}", eltk);
                        }
                        descriptors.insert(index_descript + 1, eltk.clone()); // Insert list of descriptors
                    }
                } else if let Some(descript_elt) = self.dico_l_d.get(descriptor) { // Local table D lookup
                     for eltk in descript_elt {
                        if self.affiche_descriptors {
                            println!("{}", eltk);
                        }
                        descriptors.insert(index_descript + 1, eltk.clone()); // Insert list of descriptors
                    }
                }

            } else if descriptor.starts_with("2-") {
                // F = 2 : Operator descriptor (ref in table C)
                self.descri_table_c(reader, descriptor, &mut descriptors, &mut index_descript)?;
            }
             // ... (F=1 handling to be implemented) ...


            index_descript += 1;
        }


        println!(" ** END OF DATAS **");

        println!("DATAS DESCRIPTORS NUMBER: {}", self.datas_total.len());
        println!("DATAS :");
        for (key, value) in &self.datas_total {
            if value.len() < 10 {
                println!("  {} : {:?} ({})", key, value, self.datas_unites.get(key).unwrap_or(&String::new()));
            } else {
                println!("  {} ( {} data(s))", key, value.len());
            }
        }


        reader.read_bits(4 * bytes_size)?; // (7777 =)  End of BUFR message

        println!(" ----------- END OF BUFR MESSAGE -----------");
        Ok(Some(self.datas_total.clone()))
    }


    fn section1_v2<R: Read>(&mut self, reader: &mut BitReader<R>, bytes_size: u32) -> Result<(u32, u32, u32, u32, bool), Box<dyn Error>> {
        let length_1 = reader.read_bits(3 * bytes_size)?;
        println!("Length of section 1 : {}", length_1);
        let bufr_master_table = reader.read_bits(1 * bytes_size)?;
        println!("Bufr master table : {}", bufr_master_table);
        let sub_center_id = reader.read_bits(1 * bytes_size)?;
        println!("Identification of originating/generating sub-centre : {}", sub_center_id);
        let center_id = reader.read_bits(1 * bytes_size)?;
        println!("Identification of originating/generating centre : {}", center_id);
        let update_sequence_number = reader.read_bits(1 * bytes_size)?;
        println!("Update sequence number : {}", update_sequence_number);
        let sect2_indicator = reader.read_bits(1 * bytes_size)?;
        let sect2 = sect2_indicator == 1;
        println!("Optional (1) / No Optional (0) section follows : {} ({})", sect2_indicator, if sect2 { "yes" } else { "no" });
        let data_category = reader.read_bits(1 * bytes_size)?;
        println!("Data Category (Table A) : {}", data_category);
        let data_subcategory = reader.read_bits(1 * bytes_size)?;
        println!("Data category sub-category : {}", data_subcategory);
        Ok((length_1, bufr_master_table, sub_center_id, center_id, sect2))
    }

    fn section1_v4<R: Read>(&mut self, reader: &mut BitReader<R>, bytes_size: u32) -> Result<(u32, u32, u32, u32, bool), Box<dyn Error>> {
        let length_1 = reader.read_bits(3 * bytes_size)?;
        println!("Length of section 1 : {}", length_1);
        let bufr_master_table = reader.read_bits(1 * bytes_size)?;
        println!("Bufr master table : {}", bufr_master_table);
        let center_id = reader.read_bits(2 * bytes_size)?;
        println!("Identification of originating/generating centre : {}", center_id);
        let sub_center_id = reader.read_bits(2 * bytes_size)?;
        println!("Identification of originating/generating sub-centre : {}", sub_center_id);
        let update_sequence_number = reader.read_bits(1 * bytes_size)?;
        println!("Update sequence number : {}", update_sequence_number);
        let sect2_indicator = reader.read_bits(1 * bytes_size)?;
        println!("Optional (1) / No Optional (0) section follows : {}", sect2_indicator);
        let data_category = reader.read_bits(1 * bytes_size)?;
        println!("Data Category (Table A) : {}", data_category);
        let international_data_subcategory = reader.read_bits(1 * bytes_size)?;
        println!("International data sub-category : {}", international_data_subcategory);
        let local_subcategory = reader.read_bits(1 * bytes_size)?;
        println!("Local sub-category : {}", local_subcategory);
        Ok((length_1, bufr_master_table, sub_center_id, center_id, false)) // sect2 is always false for v4?
    }


    fn section1end<R: Read>(&mut self, version: u32, length_1: u32, reader: &mut BitReader<R>, bytes_size: u32) -> Result<(), Box<dyn Error>> {
        let lim = if version == 2 { 17 } else { 22 };
        if length_1 > lim {
            println!("SECTION 1 ending : ");
            for _ in 0..(length_1 - lim) {
                let x = reader.read_bits(1 * bytes_size)?;
                println!("{}  {}", x, x as u8 as char);
            }
            println!("END OF SECTION 1");
        }
        Ok(())
    }


    fn section2<R: Read>(&mut self, reader: &mut BitReader<R>, bytes_size: u32) -> Result<(), Box<dyn Error>> {
        let length_2 = reader.read_bits(3 * bytes_size)?;
        println!("Length of section 2 : {}", length_2);
        reader.read_bits(1 * bytes_size)?; // Reserved, set to 0
        for _ in 0..(length_2 - 4) {
            let x = reader.read_bits(1 * bytes_size)?;
            println!("{}  {}", x, x as u8 as char);
        }
        println!(" END OF SECTION 2");
        Ok(())
    }

    fn load_tables(&mut self, master_table_version: u32, center_id: u32, local_table_version: u32) -> Result<(), Box<dyn Error>> {
        let table_b_path = Path::new(&self.dir_path_table).join(format!("{}{}.csv", self.fic_tab_b, master_table_version));
        match tables_b(table_b_path.to_str().unwrap_or_default()) {
            Ok(table_b_records) => {
                self.dico_m_b = dico_descriptor_b(table_b_records)?;
            }
            Err(e) => {
                println!(" ** UNABLE TO READ MASTER TABLE B {} : {}", master_table_version, e);
                self.dico_m_b = HashMap::new();
            }
        }

        let table_d_path = Path::new(&self.dir_path_table).join(format!("{}{}.csv", self.fic_tab_d, master_table_version));
        match tables_d(table_d_path.to_str().unwrap_or_default()) {
            Ok(table_d_records) => {
                self.dico_m_d = dico_descriptor_d(table_d_records)?;
            }
            Err(e) => {
                println!(" ** UNABLE TO READ MASTER TABLE D {} : {}", master_table_version, e);
                self.dico_m_d = HashMap::new();
            }
        }

        let local_table_b_path = Path::new(&self.dir_path_table).join(format!("{}{}_{}.csv", self.fic_local_tab_b, center_id, local_table_version));
        match tables_b(local_table_b_path.to_str().unwrap_or_default()) {
            Ok(local_table_b_records) => {
                self.dico_l_b = dico_descriptor_b(local_table_b_records)?;
            }
            Err(e) => {
                println!(" ** UNABLE TO READ LOCAL TABLE B {} _ {} : {}", center_id, local_table_version, e);
                self.dico_l_b = HashMap::new();
            }
        }

        let local_table_d_path = Path::new(&self.dir_path_table).join(format!("{}{}_{}.csv", self.fic_local_tab_d, center_id, local_table_version));
        match tables_d(local_table_d_path.to_str().unwrap_or_default()) {
            Ok(local_table_d_records) => {
                self.dico_l_d = dico_descriptor_d(local_table_d_records)?;
            }
            Err(e) => {
                println!(" ** UNABLE TO READ LOCAL TABLE D {} _ {} : {}", center_id, local_table_version, e);
                self.dico_l_d = HashMap::new();
            }
        }
        Ok(())
    }


}


fn main() -> Result<(), Box<dyn Error>> {
    println!("Hello, world!");

    let dir_path_table = "/Users/adrienbufort/Documents/workspace/meteolibre_dataset/tables".to_string(); // Replace with your actual path
    let fic_tab_b = "bufrtabb_{}".to_string();
    let fic_tab_d = "bufrtabd_{}".to_string();
    let fic_local_tab_b = "localtabb_{}_{}".to_string();
    let fic_local_tab_d = "localtabd_{}_{}".to_string();
    let affiche_descriptors = true;

    let mut decoder = BufrDecoder::new(dir_path_table, fic_tab_b, fic_tab_d, fic_local_tab_b, fic_local_tab_d, affiche_descriptors);
    let file_path = "/Users/adrienbufort/Documents/workspace/meteolibre_dataset/T_IMFR27_C_LFPW_20241228120000.bufr"; // Replace with your BUFR file path

    let input_file = File::open(file_path)?;
    let mut reader = BitReader::new(BufReader::new(input_file));

    while let Some(_datas_total) = decoder.decode_bufr_message(&mut reader, 8)? {
         // Process datas_total if needed
    }


    println!(" END OF FILE ");


    Ok(())
}
