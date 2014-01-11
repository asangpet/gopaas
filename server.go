package main
import (
    "bytes"
    "encoding/json"
    "log"
    "net/http"
    "os"
    "os/exec"
    "strings"
    "time"
    "github.com/fsouza/go-dockerclient"
    "github.com/codegangsta/martini"
    dockerApi "github.com/dotcloud/docker"
    "github.com/dotcloud/docker/pkg/namesgenerator"
    "labix.org/v2/mgo"
    //"labix.org/v2/mgo/bson"
)

type Instance interface {
    Container(p *PaasController) *dockerApi.Container
    Initialize()
}

type BaseInstance struct {
    ContainerId string
    IPAddress string
}

func NewInstance(container *dockerApi.Container) *BaseInstance {
    instance := BaseInstance{container.ID, container.NetworkSettings.IPAddress}
    return &instance
}

func (i *BaseInstance) Container(p *PaasController) *dockerApi.Container {
    container, err := p.Client.InspectContainer(i.ContainerId)
    if err != nil {
        log.Println(err)
    }
    return container
}

func (i *BaseInstance) Initialize() {}

type MongoInstance struct {
    BaseInstance
}

func (i *MongoInstance) ConnectWithInfo(address string, direct bool) *mgo.Session {
    var session *mgo.Session
    var err error

    // validate DB session
    var dialURL string
    if direct {
        dialURL = strings.Join([]string{"mongodb://",address,"?connect=direct"},"")
    } else {
        dialURL = address
    }

    for ;session == nil;session, err = mgo.Dial(dialURL) {
        if err != nil {
            log.Println(err)
        }
    }
    log.Println("Obtained mongo session")

    return session
}

func (i *MongoInstance) Connect() *mgo.Session {
    return i.ConnectWithInfo(i.IPAddress, true)
}

type ReplMemberDoc struct {
    ID int `json:"_id"`
    Host string `json:"host"`
}
type ReplConfigDoc struct {
    ID string `json:"_id"`
    Members []ReplMemberDoc `json:"members"`
    Version int `json:"version,omitempty"`
}

type writer func(fo *os.File)

func writeCommand(fname string, w writer) {
    fo,err := os.Create(fname)
    if err != nil {
        log.Println(err)
    }
    defer fo.Close()
    w(fo)
}

func (i *MongoInstance) InitSet() {
    var config ReplConfigDoc
    config.ID = "rs0"
    config.Members = []ReplMemberDoc{{0,i.IPAddress}}
    out,err := json.Marshal(config)
    log.Printf("Marshalling config:%s",out)

    fname := "initRepl."+i.IPAddress+".js"
    writeCommand(fname, func(fo *os.File) {
        fo.WriteString("print(tojson(db.runCommand({replSetInitiate:")
        fo.Write(out)
        fo.WriteString("})))")
    })
    defer func(name string) {
        os.Remove(fname)
    }(fname)

    cmd := exec.Command("mongo","--quiet",i.IPAddress+"/admin",fname)

    out, err = cmd.Output()
    if err != nil {
        log.Println(err)
        return
    }
    log.Printf("output:%s",out)

    type result struct {
        Ok int
        Info2 string
        Me string
        Info string
    }

    res := result{}
    dec := json.NewDecoder(bytes.NewReader(out))
    if err := dec.Decode(&res); err != nil {
         log.Println(err)
    }
    log.Println("Run result:",res)
}

func runMongo(db string, fname string, v interface{}) {
    cmd := exec.Command("mongo","--quiet",db,fname)

    out, err := cmd.Output()
    if err != nil {
        log.Println(err)
        return
    }
    log.Printf("output:%s",out)

    dec := json.NewDecoder(bytes.NewReader(out))
    if err := dec.Decode(v); err != nil {
         log.Println(err)
    }
}

func (i *MongoInstance) JoinCluster(primaryIP string) {
    log.Println("Joining to cluster", primaryIP)

    fname := "getRepl."+primaryIP+".js"
    writeCommand(fname, func(fo *os.File) {
        fo.WriteString("print(tojson(rs.conf()))")
    })
    defer func(name string) {
        os.Remove(fname)
    }(fname)

    var config ReplConfigDoc
    runMongo(primaryIP, fname, &config)
    log.Println("Existing cluster",config)

    config.Members = append(config.Members, ReplMemberDoc{len(config.Members),i.IPAddress})
    config.Version++

    out,_ := json.Marshal(config)
    log.Printf("Marshalling config:%s",out)

    fname = "reconfRepl."+i.IPAddress+".js"
    writeCommand(fname, func(fo *os.File) {
        fo.WriteString("print(tojson(db.runCommand({replSetReconfig:")
        fo.Write(out)
        fo.WriteString(",force:false})))")
    })
    defer func(name string) {
        os.Remove(fname)
    }(fname)

    type result struct {
        Ok int
        Errmsg string
        Info2 string
        Me string
        Info string
    }

    res := result{}
    runMongo(primaryIP + "/admin", fname, &res)
}

type PaasController struct {
    Client *docker.Client
    Instances map[string]Instance

    PrimaryIP string
}

func NewPaasController(clientUrl string) (*PaasController, error) {
    p := new(PaasController)
    var err error
    p.Client, err = docker.NewClient(clientUrl)
    p.Instances = make(map[string]Instance)
    return p, err
}

func (p *PaasController) Exists(name string) bool {
    if _, present := p.Instances[name]; present {
        return true
    } else {
        return false
    }
}

// Scan existing containers and load it into the system
func (p *PaasController) RescanContainers() []dockerApi.APIContainers{
    opts := docker.ListContainersOptions{}
    opts.All = true
    containers, err := p.Client.ListContainers(opts)
    if err != nil {
        log.Println(err)
    }

    for _, value := range containers {
        name := value.Names[0][1:]
        if _, present := p.Instances[name]; !present {
            container, _ := p.Client.InspectContainer(value.ID)
            p.Instances[name] = NewInstance(container)
        }
    }

    return containers
}

// Perform rescan to scan existing containers
func (p *PaasController) PerformRescanContainers(res http.ResponseWriter, log *log.Logger) {
    res.WriteHeader(200)
    enc := json.NewEncoder(res)

    result := map[string][]dockerApi.APIContainers{"containers":p.RescanContainers()}
    if err := enc.Encode(result); err != nil {
        log.Println(err)
    }
}

func (p *PaasController) ListInstances(res http.ResponseWriter) {
    res.WriteHeader(200)
    enc := json.NewEncoder(res)
    if err := enc.Encode(p.Instances); err != nil {
        log.Println(err)
    }
}

func (p *PaasController) Inspect(params martini.Params, req *http.Request, res http.ResponseWriter, log *log.Logger) {
    id := params["id"]

    container, err := p.Client.InspectContainer(id)
    if err != nil {
        log.Println(err)
    }

    res.WriteHeader(200)
    enc := json.NewEncoder(res)
    enc.Encode(container)
}

// Create a new container instance and add it to the system
func (p *PaasController) AddInstance(name string) (Instance, error) {
    opts := docker.CreateContainerOptions{name}
    config := dockerApi.Config{}
    config.Image = "asangpet/mongodb"
    config.Cmd = []string{"--replSet","rs0","--smallfiles","--oplogSize","16"}

    log.Println("Creating container: ", name)
    container,err := p.Client.CreateContainer(opts, &config)
    if err != nil {
        log.Println("Failed to create container:", err)
    }

    hostConfig := dockerApi.HostConfig{}
    if err := p.Client.StartContainer(name, &hostConfig); err != nil {
        log.Println("Failed to launch container")
    }

    container, err = p.Client.InspectContainer(container.ID)
    if err != nil {
        log.Println("Failed to get container info:", container.ID)
    }

    p.Instances[name] = &MongoInstance{BaseInstance{container.ID, container.NetworkSettings.IPAddress}}
    return p.Instances[name], err
}

func (p *PaasController) CreateContainer(res http.ResponseWriter, req *http.Request, log *log.Logger) {
    type Message struct {
        Name string
    }

    dec := json.NewDecoder(req.Body)
    var m Message
    if err := dec.Decode(&m); err != nil {
        log.Println(err)
    }

    var name string
    _, present := p.Instances[m.Name]
    if m.Name == "" || present {
        name, _ = namesgenerator.GenerateRandomName(p)
    } else {
        name = m.Name
    }

    instance, err := p.AddInstance(name)
    if err != nil {
        res.WriteHeader(500)
        log.Println(err)
    } else {
        res.WriteHeader(201)
        enc := json.NewEncoder(res)
        if err := enc.Encode(instance); err != nil {
            log.Println(err)
        }
    }
}

func (p *PaasController) NewCluster(res http.ResponseWriter, req *http.Request, log *log.Logger) {
    start := time.Now().UnixNano()

    name, _ := namesgenerator.GenerateRandomName(p)
    instance, _ := p.AddInstance(name)

    log.Println("[Profile] Create instance(ns):", time.Now().UnixNano() - start)

    switch instance := instance.(type) {
    default:
        log.Printf("Undefined action for instance type %T", instance)
    case *MongoInstance:
        log.Println("Initializing Mongo instance", instance.IPAddress)

        session := instance.Connect()
        session.Close()

        log.Println("[Profile] Server startup(ns):", time.Now().UnixNano() - start)

        instance.InitSet()
        log.Println("Setting primary IP", p.PrimaryIP)
        p.PrimaryIP = instance.IPAddress

        log.Println("[Profile] Service ready(ns):", time.Now().UnixNano() - start)
    }

    res.WriteHeader(200)
}

func (p *PaasController) Grow(res http.ResponseWriter, req *http.Request, log *log.Logger) {
    start := time.Now().UnixNano()

    name, _ := namesgenerator.GenerateRandomName(p)
    instance, _ := p.AddInstance(name)

    log.Println("[Profile] Create instance(ns):", time.Now().UnixNano() - start)

    switch instance := instance.(type) {
    default:
        log.Printf("Undefined action for instance type %T", instance)
    case *MongoInstance:
        log.Println("Initializing Mongo instance", instance.IPAddress)

        session := instance.Connect()
        session.Close()

        log.Println("[Profile] Server startup(ns):", time.Now().UnixNano() - start)
        instance.JoinCluster(p.PrimaryIP)
        log.Println("[Profile] Service ready(ns):", time.Now().UnixNano() - start)
    }
    res.WriteHeader(200)
}

func main() {
    m := martini.Classic()
    paas, err := NewPaasController("unix:/var/run/docker.sock")
    if err != nil {
        log.Fatal(err)
    }

    paas.RescanContainers()

    m.Get("/rescan", paas.PerformRescanContainers)
    m.Get("/instances", paas.ListInstances)
    m.Post("/", paas.CreateContainer)
    m.Get("/container/:id", paas.Inspect)
    m.Get("/mongo", func() string {
        return "Mongo!"
    })
    m.Post("/grow", paas.Grow)
    m.Post("/cluster", paas.NewCluster)
    m.Run()
}
