use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    fs::{self, File},
    io::{BufReader, BufWriter},
    path::Path,
    time::Instant,
};

use geo::{coord, point, HaversineDistance};
use itertools::Itertools;
use osmpbfreader::{Node, NodeId, OsmObj, OsmPbfReader, Way};

const ADJ_LIST_JSON_PATH: &str = "data/adj-list.json";
const NODES_JSON_PATH: &str = "data/nodes.json";
const PBF_PATH: &str = "data/japan-latest.osrm.pbf";
const RESULT_PATH: &str = "data/result-polyline.txt";

const INACCESSIBLE_TAGS: [(&str, &str); 7] = [
    ("highway", "motorway"),
    ("highway", "motorway_link"),
    // ref: https://github.com/team-azb/route-bucket-backend/blob/master/osrm/customized.lua#L54
    ("access", "agricultural"),
    ("access", "delivery"),
    ("access", "forestry"),
    ("access", "delivery"),
    ("access", "use_sidepath"),
];

const START_NODE_ID: NodeId = NodeId(5798366045); // https://www.openstreetmap.org/node/5798366045
const GOAL_NODE_ID: NodeId = NodeId(1254449298); // https://www.openstreetmap.org/node/1254449298

fn download_pbf(pbf_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut resp =
        reqwest::blocking::get("https://download.geofabrik.de/asia/japan-latest.osm.pbf")?;
    let mut pbf_file = BufWriter::new(File::create(pbf_path)?);
    resp.copy_to(&mut pbf_file)?;
    Ok(())
}

fn is_cyclable_way(way: &Way) -> bool {
    let is_road = way.tags.contains_key("highway");
    let is_paved = !way.tags.contains_key("surface")
        || way.tags.contains("surface", "paved")
        || way.tags.contains("surface", "asphalt")
        || way.tags.contains("surface", "concrete")
        || way.tags.contains("surface", "paving_stones");
    let is_accessible = INACCESSIBLE_TAGS
        .iter()
        .all(|(key, value)| !way.tags.contains(key, value));

    is_road && is_paved && is_accessible
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let timer = Instant::now();

    let mut nodes = HashMap::<NodeId, Node>::new();
    let mut adj_list = HashMap::<NodeId, Vec<(NodeId, f64)>>::new();
    let nodes_json_path = Path::new(NODES_JSON_PATH);
    let adj_list_json_path = Path::new(ADJ_LIST_JSON_PATH);

    if nodes_json_path.exists() && adj_list_json_path.exists() {
        nodes = serde_json::from_reader(BufReader::new(File::open(nodes_json_path)?))?;
        adj_list = serde_json::from_reader(BufReader::new(File::open(adj_list_json_path)?))?;
    } else {
        let pbf_path = Path::new(PBF_PATH);
        if !pbf_path.exists() {
            download_pbf(pbf_path).or_else(|err| {
                fs::remove_file(pbf_path)?;
                Err(err)
            })?;
        }

        let mut pbf_reader = OsmPbfReader::new(BufReader::new(File::open(PBF_PATH)?));
        let mut ways = Vec::<Way>::new();
        for osm_obj in pbf_reader.par_iter().map(Result::unwrap) {
            match osm_obj {
                OsmObj::Node(node) => {
                    if nodes.len() == 0 {
                        println!(
                            "First node: ({}, {}), {:?}",
                            node.lat(),
                            node.lon(),
                            node.tags
                        );
                    }
                    nodes.insert(node.id, node);
                }
                OsmObj::Way(way) => {
                    if !is_cyclable_way(&way) {
                        continue;
                    }
                    if ways.len() == 0 {
                        println!("First way: ({:?})", way);
                    }
                    ways.push(way);
                }
                _ => {}
            }
        }

        println!(
            "Pre computation done: {} nodes, {} ways ({}s)",
            nodes.len(),
            ways.len(),
            timer.elapsed().as_secs_f64()
        );

        let mut node_ids = HashSet::<NodeId>::new();
        for way in ways {
            let is_bidirectional = !way.tags.contains("oneway", "yes");
            way.nodes.iter().tuple_windows().for_each(|(&u, &v)| {
                node_ids.insert(u);
                node_ids.insert(v);

                let edge_len = point!(x: nodes[&u].lon(), y: nodes[&u].lat())
                    .haversine_distance(&point!( x: nodes[&v].lon(), y: nodes[&v].lat()));

                adj_list.entry(u).or_insert(Vec::new()).push((v, edge_len));
                if is_bidirectional {
                    adj_list.entry(v).or_insert(Vec::new()).push((u, edge_len));
                }
            });
        }

        nodes = nodes
            .into_iter()
            .filter(|(id, _)| node_ids.contains(id))
            .collect();

        serde_json::to_writer_pretty(BufWriter::new(File::create(nodes_json_path)?), &nodes)?;
        serde_json::to_writer_pretty(BufWriter::new(File::create(adj_list_json_path)?), &adj_list)?;
    }

    println!(
        "Graph loaded: {} nodes, {} edges ({} s)",
        nodes.len(),
        adj_list.values().map(|e| e.len()).sum::<usize>(),
        timer.elapsed().as_secs_f64()
    );

    let mut queue = BinaryHeap::new();
    let mut parent = HashMap::<NodeId, NodeId>::new();
    queue.push(Reverse((0u64, START_NODE_ID)));
    parent.insert(START_NODE_ID, NodeId(-1));
    while let Some(Reverse((dist, current))) = queue.pop() {
        if current == GOAL_NODE_ID {
            println!(
                "GOOOOOAL!!! ({} s) Dist: {}",
                timer.elapsed().as_secs_f64(),
                (dist as f64) / 1000.0
            );
            break;
        }
        if !adj_list.contains_key(&current) {
            continue;
        }

        for (neighbor, edge_len) in &adj_list[&current] {
            if parent.contains_key(neighbor) {
                continue;
            }
            queue.push(Reverse((
                dist + (edge_len * 1000.0).round() as u64,
                *neighbor,
            )));
            parent.insert(*neighbor, current);
        }
    }

    let mut cur_id = GOAL_NODE_ID;
    let mut coords = VecDeque::new();
    coords.push_front(coord! {
        x: nodes[&GOAL_NODE_ID].lon(),
        y: nodes[&GOAL_NODE_ID].lat()
    });
    while cur_id != START_NODE_ID {
        cur_id = parent[&cur_id];
        coords.push_front(coord! {
            x: nodes[&cur_id].lon(),
            y: nodes[&cur_id].lat()
        });
    }
    println!("Dijkstra completed ({} s)!", timer.elapsed().as_secs_f64());

    Ok(fs::write(
        RESULT_PATH,
        polyline::encode_coordinates(coords, 5).unwrap(),
    )?)
}
