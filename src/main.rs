use std::{
    fs::{self, File},
    path::Path,
    time::Instant,
};

use osmpbfreader::{OsmObj, OsmPbfReader};

// fn is_cycling_node(node: Node) -> bool {
//     node.tags.contains(key, value)
// }

const PBF_PATH: &str = "data/japan-latest.osrm.pbf";

fn download_pbf(pbf_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut resp =
        reqwest::blocking::get("https://download.geofabrik.de/asia/japan-latest.osm.pbf")?;
    let mut pbf_file = File::create(pbf_path)?;
    resp.copy_to(&mut pbf_file)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = Path::new(PBF_PATH);
    if !path.exists() {
        download_pbf(path).or_else(|err| {
            fs::remove_file(path)?;
            Err(err)
        })?;
    }

    let timer = Instant::now();

    let mut num_nodes = 0;
    let mut num_edges = 0;
    let mut pbf_reader = OsmPbfReader::new(File::open(PBF_PATH)?);
    for osm_obj in pbf_reader.par_iter().map(Result::unwrap) {
        match osm_obj {
            OsmObj::Node(node) => {
                if num_nodes == 0 {
                    println!("First node: ({}, {})", node.lat(), node.lon());
                }
                num_nodes += 1;
            }
            OsmObj::Way(way) => {
                num_edges += way.nodes.len() - 1;
            }
            _ => {}
        }
    }

    println!(
        "Result: {} nodes, {} edges ({}s)",
        num_nodes,
        num_edges,
        timer.elapsed().as_secs_f64()
    );
    Ok(())
}
