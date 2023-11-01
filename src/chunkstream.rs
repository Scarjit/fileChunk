use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::Write;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use crate::rolling_hash::RabinFingerprint;

const CHUNK_MODULUS:u64 = 1024*1024;

#[derive(Debug, Clone)]
pub(crate) struct Chunk{
    current_offset: u64,
    buffer: Vec<u8>,
    base: ChunkBase
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ChunkBase{
    files: Vec<ChunkFile>,
    fingerprint: RabinFingerprint,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RestoreInformation{
    files: HashMap<String, IndexMap<String, StartEndTuple>>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StartEndTuple{
    start: u64,
    end: u64,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ChunkFile{
    name: String,
    start: u64,
    end: u64,
    filename: String,
}

impl Chunk{
    pub(crate) fn new() -> Chunk{
        Chunk{
            current_offset: 0,
            buffer: vec![],
            base: ChunkBase{
                files: vec![],
                fingerprint: RabinFingerprint::new(),
            }
        }
    }

    pub(crate) fn add_file(&mut self, file : &str, bytes: &Vec<u8>) -> Vec<u8>{
        let mut vec_dq_bytes = VecDeque::from(bytes.to_vec());
        let mut written = 0;
        for _ in 0..bytes.len(){
            let byte = vec_dq_bytes.pop_front().unwrap();
            self.buffer.push(byte);
            self.base.fingerprint.push_byte(byte);
            written += 1;
            if self.base.fingerprint.value() % CHUNK_MODULUS == 0{
                self.current_offset += written as u64;
                self.base.files.push(ChunkFile {
                    filename: file.to_string(),
                    name: self.base.fingerprint.value().to_string(),
                    start: self.current_offset - written as u64,
                    end: self.current_offset ,
                });
                return vec_dq_bytes.make_contiguous().to_vec();
            }
        }
        //println!("File: {}, written: {}, remaining: {}, fingerprint: {}", file, written, vec_dq_bytes.len(), self.base.fingerprint.value());

        bytes.to_vec()
    }

    fn save(&self, output_path: &str) {
        let path = format!("{}/{}.chunk", output_path,self.base.fingerprint.value());
        //println!("Saving chunk: {}", path);
        // Check if file exists
        if std::path::Path::new(&path).exists() {
            return;
        }
        let mut file = fs::File::create(path).unwrap();
        file.write_all(&self.buffer).unwrap();
    }
}

pub struct Chunker{
    bases: HashMap<String, Vec<ChunkBase>>,
}

impl Chunker{
    pub(crate) fn new() -> Chunker{
        Chunker{
            bases: HashMap::new(),
        }
    }
    pub(crate) fn add_files(mut self, mut paths: Vec<String>, output_path: &str){
        paths.sort_unstable();
        let mut chunk = Chunk::new();
        let mut remaining_bytes = vec![];
        let mut last_file = String::new();
        for path in paths.iter(){
            println!("Path: {}", path);
            let path = path.replace("\\", "/");
            // Try read file
            remaining_bytes = fs::read(&path).expect("Unable to read file");
            while !remaining_bytes.is_empty() {
                remaining_bytes = chunk.add_file(&path,&remaining_bytes);
                if chunk.base.fingerprint.value() % CHUNK_MODULUS == 0 {
                    //println!("Chunk: {}", chunk.base.fingerprint.value());
                    // save old chunk
                    self.update_restore_info(&chunk);
                    chunk.save(output_path);
                    chunk = Chunk::new();
                }
            }
            last_file = path.to_string();
        }
        assert!(remaining_bytes.is_empty());
        if !chunk.buffer.is_empty() {
            //println!("Last chunk: {}", chunk.base.fingerprint.value());
            // Save last chunk
            self.update_restore_info(&chunk);
            chunk.save(output_path);
        }

        self.dump_restore_info(output_path);
    }

    fn update_restore_info(&mut self, filename: &Chunk){
        if filename.base.files.len() > 1{
            // We need to rename the base.name for the all files, except the last one to the last one
            let last_base = filename.base.files.last().unwrap();
            let last_base_name = last_base.name.clone();
            //println!("Last base name: {}", last_base_name);
            for base in filename.base.files.iter(){
                let mut base_clone = base.clone();
                base_clone.name = last_base_name.clone();
                self.update_restore_info_for_file(&base_clone, filename);
            }
        }else {
            for base in filename.base.files.iter() {
                //println!("Updating restore info for: {}: {} {}->{}", base.name, base.filename, base.start, base.end);
                self.update_restore_info_for_file(base, filename);
            }
        }
    }

    fn update_restore_info_for_file(&mut self, file: &ChunkFile, chunk: &Chunk) {
        //println!("Updating restore info for: {}: {} {}->{}", file.name, file.filename, file.start, file.end);
        let filename = &file.filename;
        match self.bases.get(filename) {
            None => {
                self.bases.insert(filename.to_string(), vec![chunk.base.clone()]);
            }
            Some(base) => {
                let mut new_base = base.clone();
                new_base.push(chunk.base.clone());
                self.bases.insert(filename.to_string(), new_base);
            }
        }
    }

    fn dump_restore_info(&self, output_path: &str){
        let path = format!("{}/restore_info.yaml", output_path);
        let mut file = fs::File::create(path).unwrap();

        let mut restore_info = RestoreInformation{
            files: HashMap::new(),
        };

        for (filename, bases) in self.bases.iter(){
            //println!("Filename: {}", filename);
            //println!("Bases: {:?}", bases);

            let mut file_map = IndexMap::new();
            for base in bases.iter(){
                for chunk_file in base.files.iter() {
                    file_map.insert(chunk_file.name.clone(), StartEndTuple {
                        start: chunk_file.start,
                        end: chunk_file.end,
                    });
                }
            }
            restore_info.files.insert(filename.to_string(), file_map);
        }

        let yaml = serde_yaml::to_string(&restore_info).unwrap();
        file.write_all(yaml.as_bytes()).unwrap();
    }

    fn restore_file(&self, filename: &str, data_path: &str, output_path: &str){
        // Normalize filename to unix path
        let filename = filename.replace("\\", "/");

        let filename_without_leading_dot_slash = filename.trim_start_matches("./");
        let path = format!("{}/{}", output_path, filename_without_leading_dot_slash);
        // Create parent directories
        let parent = std::path::Path::new(&path).parent().unwrap();
        std::fs::create_dir_all(parent).unwrap();

        let mut file = fs::File::create(path).unwrap();

        let restore_info_path = format!("{}/restore_info.yaml", data_path);
        let restore_info = fs::read_to_string(restore_info_path).unwrap();
        let restore_info: RestoreInformation = serde_yaml::from_str(&restore_info).unwrap();

        //println!("Restoring: {}", filename);
        //println!("{:?}", restore_info);

        let file_map = restore_info.files.get(&filename).unwrap();
        for (chunk_name, start_end) in file_map.iter(){
            let chunk_path = format!("{}/{}.chunk", data_path, chunk_name);
            //println!("Chunk path: {}", chunk_path);
            let chunk_bytes = fs::read(chunk_path).unwrap();
            let chunk_bytes = chunk_bytes[start_end.start as usize..start_end.end as usize].to_vec();
            //println!("Chunk: {}", chunk_name);
            file.write_all(&chunk_bytes).unwrap();
        }
    }
}



#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_chunking() {
        // Scan ./tests/data for files
        let mut paths: Vec<String> = Vec::new();

        for entry in fs::read_dir("./tests/data").unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let path_str = path.to_str().unwrap();
            paths.push(path_str.to_string());
        }
        // Cleanup output folder
        let _ = fs::remove_dir_all("./tests/chunks");
        fs::create_dir("./tests/chunks").unwrap();

        let chunker = Chunker::new();
        chunker.add_files(paths, "./tests/chunks");

        // Attempt restore
        let _ = fs::remove_dir_all("./tests/restored");
        fs::create_dir("./tests/restored").unwrap();

        let chunker_restore = Chunker::new();

        // Check if restored files is the same as original
        for entry in fs::read_dir("./tests/data").unwrap() {
            let path = format!("./tests/data/{}", entry.unwrap().file_name().to_str().unwrap());
            let restored_path = format!("./tests/restored/{}",path);

            chunker_restore.restore_file(&path, "./tests/chunks", "./tests/restored");

            let original = fs::read(&path).unwrap();
            let restored = fs::read(&restored_path).unwrap();

            assert_eq!(original, restored);
        }
    }
}
