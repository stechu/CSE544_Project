/**************************************************
 *cse 544 q3
 **************************************************/


/****************************************************************************/
/* Header Files */

#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <utility>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <graphlab.hpp>

//#define SIGNALALL

/****************************************************************************/
/* Type Definitions */

struct paper_vertex;

typedef unsigned int vertex_depth;
typedef unsigned int paper_index;
typedef unsigned short paper_year;
typedef graphlab::vertex_id_type paper_id;

typedef std::pair<paper_index,paper_index> paper_pair;


/****************************************************************************/
/* Constants and global variables */

paper_index num_seed_papers;
std::vector<paper_index> seed_papers;
std::map<paper_id,paper_index> seed_map;

const paper_id invalid_id = -1;
const vertex_depth infinite_depth = -1;
const paper_index invalid_index = -1;
const paper_id id_cutoff = -1;


/****************************************************************************/
/* Data Structures */


// The value of b is always 1 in our algorithm. This function essentially
// increments depth a if and only if the vertex is actually a descendant. 
static inline vertex_depth depth_add(vertex_depth a, vertex_depth b)
{
    if (a == invalid_index || b == invalid_index) {
        return invalid_index;
    }
    return a + b;
}

// The value gathered on each iteration for each vertex is the vertex's
// depths or distance to each of the seed nodes.
struct gather_value {
    std::vector<vertex_depth> depths;

    // Default constructor - initiallizes all depths to infinite
    gather_value()
        : depths(num_seed_papers)
    {
        for (paper_index i = 0; i < num_seed_papers; i++) {
            depths[i] = infinite_depth;
        }
    }

    // Copy constructor (needed)
    gather_value(const gather_value &orig)
        : depths(orig.depths)
    {
    }

    // Updates the depth according to the depth of the given neighbour
    void incorporate_neighbor(const gather_value &neighbor)
    {
        for (paper_index i = 0; i < num_seed_papers; i++) {
            depths[i] = std::min(depths[i], depth_add(neighbor.depths[i], 1));
        }
    }

    vertex_depth overall_depth(paper_index a, paper_index b) const
    {
        return std::max(depths[a], depths[b]);
    }

    void save(graphlab::oarchive& oarc) const
    {
        oarc << depths;
    }

    void load(graphlab::iarchive& iarc)
    {
        iarc >> depths;
    }

    // We overload this operator to ensure that the 'total' of gather valuees
    // computed by graphlab is actually what we want i.e the new set of best
    // values for the given vertex after the iteration.
    gather_value& operator+=(const gather_value& other)
    {
        for (paper_index i = 0; i < num_seed_papers; i++) {
            depths[i] = std::min(depths[i], other.depths[i]);
        }
        return *this;
    }

    bool operator==(const gather_value& other) const
    {
        bool equal = true;
        for (paper_index i = 0; equal && i < num_seed_papers; i++) {
            equal = equal && (depths[i] == other.depths[i]);
        }
        return equal;
    }

    bool operator!=(const gather_value& other) const
    {
        return !(*this == other);
    }

    bool operator<=(const gather_value& other) const
    {
        bool lte = true;
        for (paper_index i = 0; lte && i < num_seed_papers; i++) {
            lte = lte && (depths[i] <= other.depths[i]);
        }
        return lte;
    }
    
    // Overload the operator such that it judges if any of the depths have
    // actually improved or not.
    bool operator<(const gather_value& other) const
    {
        bool lte = true;
        bool lt = false;
        for (paper_index i = 0; lte && i < num_seed_papers; i++) {
            lte = lte && (depths[i] <= other.depths[i]);
            lt = lt || (depths[i] < other.depths[i]);
        }
        return lte && lt;
    }

};

// Each vertex represents a paper
struct paper_vertex {
    paper_year year;
    gather_value gather;
    paper_index index;
    bool first_required;

    // Constructor - if seed node => sets depth to self as 0
    explicit paper_vertex (paper_year y, paper_index idx)
        : year(y), gather(), index(idx), first_required(idx != invalid_index)
    {
        if (idx != invalid_index) {
            gather.depths[idx] = 0;
        }
    }

    paper_vertex ()
        : year(0), gather(), index(invalid_index)
    {
    }

    void save(graphlab::oarchive& oarc) const
    {
        oarc << year << gather << index << first_required;
    }

    void load(graphlab::iarchive& iarc)
    {
        iarc >> year >> gather >> index >> first_required;
    }
};


typedef graphlab::distributed_graph<paper_vertex, graphlab::empty> graph_type;
typedef graph_type::vertex_type vertex_type;

// This class encapsulates the program that is run on each signalled vertex
// by graphlab on each iteration.
class paper_vertex_program :
    public graphlab::ivertex_program<graph_type, gather_value>,
    public graphlab::IS_POD_TYPE
{
    private:
        bool perform_scatter;

    public:
        // We are only interested in the incoming edges for any given vertex
        edge_dir_type gather_edges(
            icontext_type& context,
            const vertex_type& vertex) const
        {
            return graphlab::IN_EDGES;
        }

        // This function is called once for every edge selected in the gather
        // edges function.
        gather_value gather(
            icontext_type& context,
            const vertex_type& vertex,
            edge_type& edge) const
        {
            gather_value gv = edge.target().data().gather;
            /*std::cout << "Gather: on " << edge.target().id() << std::endl;
            std::cout << "Gather: from " << edge.source().id() << std::endl;
            for (paper_index i = 0; i < num_seed_papers; i++) {
                std::cout << gv.depths[i] << " ";
            }
            std::cout << std::endl;
            for (paper_index i = 0; i < num_seed_papers; i++) {
                std::cout << edge.source().data().gather.depths[i] << " ";
            }
            std::cout << std::endl;*/

            gv.incorporate_neighbor(edge.source().data().gather);

            /*std::cout << "Gather result: on " << edge.target().id() << " ";
            for (paper_index i = 0; i < num_seed_papers; i++) {
                std::cout << gv.depths[i] << " ";
            }
            std::cout << std::endl;*/
            return gv;
        }

        // This is called after all gather functions are complete. Total is the
        // sum (overloaded operator) of all gather values returned by gather
        // function.
        void apply(
            icontext_type& context,
            vertex_type& vertex,
            const gather_type& total)
        {
            //std::cout << "apply" << std::endl;
            if (total < vertex.data().gather) {
                // Something changed
                vertex.data().gather = total;
                perform_scatter = true;
            } else {
                perform_scatter = false;
            }
#ifndef SIGNALALL
            if (vertex.data().first_required) {
                perform_scatter = true;
                vertex.data().first_required = false;
            }
#endif
        }

        // Marks which edges are to be signalled for the next iteration. If 
        // perform scatter is true i.e a change was observed in any of the 
        // depths, we signal all outoging edges to propogate the change(s). 
        // Otherwise dont signal anything.
        edge_dir_type scatter_edges(
            icontext_type& context,
            const vertex_type& vertex) const
        {
            return (perform_scatter ? graphlab::OUT_EDGES : graphlab::NO_EDGES);
        }

        // Called once for all edges selected in scatter_edges function.
        void scatter(
            icontext_type& context,
            const vertex_type& vertex,
            edge_type& edge) const
        {
            context.signal(edge.target());
        }
};

struct fold_paper {
    paper_id id;
    paper_year year;
    vertex_depth depth;

    fold_paper()
        : id(-1), year(0), depth(invalid_index)
    {
    }

    fold_paper(paper_id i, paper_year y, vertex_depth d)
        : id(i), year(y), depth(d)
    {
    }

    void save(graphlab::oarchive& oarc) const
    {
        oarc << id << year << depth;
    }

    void load(graphlab::iarchive& iarc)
    {
        iarc >> id >> year >> depth;
    }

    bool operator<(const fold_paper& other) const
    {
        return
            (depth < other.depth ||
                (depth == other.depth &&
                    (year < other.year ||
                     (year == other.year && id < other.id))));
    }
};

struct fold_value {
    std::map<paper_pair,fold_paper> pairs;

    fold_value()
        : pairs()
    {
    }

    void add_vertex(const vertex_type& pv)
    {
        const paper_vertex& p = pv.data();

        for (paper_index a = 0; a < num_seed_papers; a++) {
            for (paper_index b = a + 1; b < num_seed_papers; b++) {
                vertex_depth d = p.gather.overall_depth(a, b);

                // Nothing to do here, not an ancestor of this pair
                if (d == infinite_depth) {
                    continue;
                }

                fold_paper new_fp = fold_paper(pv.id(), p.year, d);
                add_pair(a, b, new_fp);
            }
        }
    }

    void add_pair(paper_index a, paper_index b, const fold_paper& fp)
    {
        paper_pair pp = paper_pair(a, b);

        std::map<paper_pair,fold_paper>::const_iterator it = pairs.find(pp);
        if (it == pairs.end() || fp < it->second) {
            pairs[pp] = fp;
        }

    }

    fold_value& operator+=(const fold_value& other)
    {
        std::map<paper_pair,fold_paper>::const_iterator it;
        for (it = other.pairs.begin(); it != other.pairs.end(); it++) {
            add_pair(it->first.first, it->first.second, it->second);
        }
        return *this;
    }

    void save(graphlab::oarchive& oarc) const
    {
        oarc << pairs;
    }

    void load(graphlab::iarchive& iarc)
    {
        iarc >> pairs;
    }
};

void fold_vertices(const vertex_type& pv, fold_value& total)
{
    total.add_vertex(pv);
}


/****************************************************************************/
/* Parser Functions */

// Loads the set of seed papers to execute for from the given file
bool load_seed_papers(const char *path)
{

    std::ifstream infile(path);
    std::string textline;
    std::vector<std::string> strs;

    // Eat up first line with title
    if (!std::getline(infile, textline)) {
        std::cerr << "Reading seed paper failed" << std::endl;
        return false;
    }

    // Read one line for each seed paper to get the id
    seed_papers.reserve(num_seed_papers);
    for (paper_index i = 0; i < num_seed_papers; i++) {
        if (!std::getline(infile, textline)) {
            std::cerr << "Reading seed paper failed" << std::endl;
            return false;
        }

        boost::split(strs, textline, boost::is_any_of(","));
        if (strs.size() != 2) {
            std::cerr << "Not 2 parts" << std::endl;
            return false;
        }

        try {
            paper_index pid = boost::lexical_cast<paper_id>(strs[0]);
            seed_papers[i] = pid;
            seed_map[pid] = i;
        } catch(const boost::bad_lexical_cast &) {
	  std::cerr << "exception[seed] " << strs[0] << std::endl;
            return false;
        }
    }
    infile.close();

    return true;
}


// Parses one line of papers.csv
bool papers_line_parser(
        graph_type& graph,
        const std::string& filename,
        const std::string& textline)
{
    std::vector<std::string> strs;
    std::map<paper_id,paper_index>::const_iterator pit;
    paper_id pid;
    paper_year pyear;
    paper_index idx;

    boost::split(strs, textline, boost::is_any_of(","));
    if (strs.size() != 2) {
        std::cerr << "Not 2 parts" << std::endl;
        return true;
    }

    try {
        pid = boost::lexical_cast<paper_id>(strs[0]);
        pyear = boost::lexical_cast<paper_year>(strs[1]);
    } catch(const boost::bad_lexical_cast &) {
      std::cerr << "exceptionp[papers]" << strs[0] << "," << strs[1] << std::endl;
        return true;
    }

    if (id_cutoff != invalid_id && pid >= id_cutoff) {
        std::cerr << "above cutoff" << std::endl;
        return true;
    }

    idx = invalid_index;
    pit = seed_map.find(pid);
    if (pit != seed_map.end()) {
        idx = pit->second;
    }
    graph.add_vertex(pid, paper_vertex(pyear, idx));

    return true;
}


// Parses one line of cites.csv
bool cites_line_parser(
        graph_type& graph,
        const std::string& filename,
        const std::string& textline)
{
    std::vector<std::string> strs;
    paper_id pa, pb;

    boost::split(strs, textline, boost::is_any_of(","));
    if (strs.size() != 2) {
        return true;
    }

    try {
        pa = boost::lexical_cast<paper_id>(strs[0]);
        pb = boost::lexical_cast<paper_id>(strs[1]);
    } catch(const boost::bad_lexical_cast &) {
        return true;
    }

    if (id_cutoff != invalid_id && (pa >= id_cutoff || pb >= id_cutoff)) {
        return true;
    }

    // self-edges don't make much sense
    if (pa == pb) {
        return true;
    }

    graph.add_edge(pa, pb);
    return true;
}

bool is_seed_vertex(const graph_type::vertex_type& vertex)
{
    return vertex.data().index != invalid_index;
}

int main(int argc, char** argv) {
    graphlab::mpi_tools::init(argc, argv);

    if (argc != 2) {
        std::cerr << "Usage: 550_app N" << std::endl;
        std::cerr << "  N = Number of seed papers" << std::endl;

        graphlab::mpi_tools::finalize();
        return -1;
    }

    num_seed_papers = std::atoi(argv[1]);
    std::cout << "Seed papers: " << num_seed_papers << std::endl;
    if (!load_seed_papers("/home/ubuntu/papers.csv")) {
        graphlab::mpi_tools::finalize();
        return -1;
    }


    graphlab::distributed_control dc;

    // Load the graph
    graph_type graph(dc);
    graph.load("/home/ubuntu/data/papers.csv", papers_line_parser);
    graph.load("/home/ubuntu/data/cites.csv", cites_line_parser);
    graph.finalize();
    std::cerr << "Graph finalized!" << std::endl;

    // Prepare execution
    graphlab::omni_engine<paper_vertex_program> engine(dc, graph, "async");
#ifdef SIGNALALL
    // Start by signalling all nodes
    engine.signal_all();
#else
    // Only signal the seed nodes
    engine.signal_vset(graph.select(is_seed_vertex));
#endif
    std::cerr << "Signalled!" << std::endl;
    engine.start();
    std::cerr << "Started!" << std::endl;

    fold_value fv = graph.fold_vertices<fold_value>(fold_vertices);
    std::cerr << "Folded!" << std::endl;
    std::ofstream outfile("/tmp/output", std::ios::trunc);
    std::map<paper_pair,fold_paper>::const_iterator it;
    outfile << "p1,p2,a,depth,year" << std::endl;
    for (it = fv.pairs.begin(); it != fv.pairs.end(); it++) {
        outfile << seed_papers[it->first.first] << ","
            << seed_papers[it->first.second] << "," << it->second.id << ","
            << it->second.depth << "," << it->second.year << std::endl;
    }
    outfile.close();
    std::cerr << "Done!" << std::endl;


    graphlab::mpi_tools::finalize();
    return 0;
}
