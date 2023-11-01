use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::Write;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use crate::rolling_hash::RabinFingerprint;

const CHUNK_MODULUS:u64 = 10;

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

    pub(crate) fn add_file(&mut self, bytes: &[u8]) -> Vec<u8>{
        let mut vec_dq_bytes = VecDeque::from(bytes.to_vec());
        let mut written = 0;
        for _ in 0..bytes.len(){
            let byte = vec_dq_bytes.pop_front().unwrap();
            self.buffer.push(byte);
            self.base.fingerprint.push_byte(byte);
            if self.base.fingerprint.value() % CHUNK_MODULUS == 0{
                break;
            }
            written += 1;
        }

        self.base.files.push(ChunkFile{
            name: self.base.fingerprint.value().to_string(),
            start: self.current_offset,
            end: self.current_offset + written as u64,
        });
        self.current_offset += written as u64;

        return vec_dq_bytes.make_contiguous().to_vec();
    }

    fn save(&self, output_path: &str) {
        let path = format!("{}/{}.chunk", output_path,self.base.fingerprint.value());
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
    pub(crate) fn add_files(mut self, paths: Vec<String>, output_path: &str){
        let mut chunk = Chunk::new();
        let mut remaining_bytes = vec![];
        let mut last_file = String::new();
        for path in paths.iter(){
            let path = path.replace("\\", "/");
            // Try read file
            let mut bytes = fs::read(&path).expect("Unable to read file");

            remaining_bytes.append(&mut bytes);
            while !remaining_bytes.is_empty() {
                remaining_bytes = chunk.add_file(&remaining_bytes);
                if chunk.base.fingerprint.value() % CHUNK_MODULUS == 0 {
                    // save old chunk
                    chunk.save(output_path);
                    self.update_restore_info(&path, &chunk);
                    chunk = Chunk::new();
                }
            }
            last_file = path.to_string();
        }
        if !chunk.buffer.is_empty() {
            // Save last chunk
            chunk.save(output_path);
            self.update_restore_info(&last_file, &chunk);
        }
        self.dump_restore_info(output_path);
    }

    fn update_restore_info(&mut self, filename: &str, chunk: &Chunk){
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
            let mut file_map = IndexMap::new();
            for base in bases.iter(){
                for chunk_file in base.files.iter(){
                    file_map.insert(chunk_file.name.clone(), StartEndTuple{
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

        println!("Restoring: {}", filename);
        println!("{:?}", restore_info);

        let file_map = restore_info.files.get(&filename).unwrap();
        for (chunk_name, start_end) in file_map.iter(){
            let chunk_path = format!("{}/{}.chunk", data_path, chunk_name);
            let chunk_bytes = fs::read(chunk_path).unwrap();
            let chunk_bytes = chunk_bytes[start_end.start as usize..=start_end.end as usize].to_vec();
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
        chunker_restore.restore_file("./tests/data/A.txt", "./tests/chunks", "./tests/restored");

        // Check if restored file is the same as original
        let original = fs::read("./tests/data/A.txt").unwrap();
        let restored = fs::read("./tests/restored/tests/data/A.txt").unwrap();

        assert_eq!(original, restored);
    }
}
