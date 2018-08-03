package commands

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"

	cmds "github.com/ipfs/go-ipfs/commands"
	"github.com/ipfs/go-ipfs/core"
	e "github.com/ipfs/go-ipfs/core/commands/e"
	path "gx/ipfs/QmYKNMEUK7nCVAefgXF1LVtZEZg3uRmBqiae4FJRXDNAyJ/go-path"

	cid "gx/ipfs/QmYVNvtQkeZ6AKSwDrjQTs432QtL6umrrK41EBq3cu7iSP/go-cid"
	ipld "gx/ipfs/QmZtNq8dArGfnpCZfx2pUNY7UcjGhVp5qqwQ4hH6mpTMRQ/go-ipld-format"
	"gx/ipfs/QmdE4gMduCKCGAcczM2F5ioYDfdeKuPix138wrES1YSr7f/go-ipfs-cmdkit"
)

// KeyList is a general type for outputting lists of keys
type KeyList struct {
	Keys []*cid.Cid
}

// KeyListTextMarshaler outputs a KeyList as plaintext, one key per line
func KeyListTextMarshaler(res cmds.Response) (io.Reader, error) {
	out, err := unwrapOutput(res.Output())
	if err != nil {
		return nil, err
	}

	output, ok := out.(*KeyList)
	if !ok {
		return nil, e.TypeErr(output, out)
	}

	buf := new(bytes.Buffer)
	for _, key := range output.Keys {
		buf.WriteString(key.String() + "\n")
	}
	return buf, nil
}

var RefsCmd = &cmds.Command{
	Helptext: cmdkit.HelpText{
		Tagline: "List links (references) from an object.",
		ShortDescription: `
Lists the hashes of all the links an IPFS or IPNS object(s) contains,
with the following format:

  <link base58 hash>

NOTE: List all references recursively by using the flag '-r'.
`,
	},
	Subcommands: map[string]*cmds.Command{
		"local": RefsLocalCmd,
	},
	Arguments: []cmdkit.Argument{
		cmdkit.StringArg("ipfs-path", true, true, "Path to the object(s) to list refs from.").EnableStdin(),
	},
	Options: []cmdkit.Option{
		cmdkit.StringOption("format", "Emit edges with given format. Available tokens: <src> <dst> <linkname>.").WithDefault("<dst>"),
		cmdkit.BoolOption("edges", "e", "Emit edge format: `<from> -> <to>`."),
		cmdkit.BoolOption("unique", "u", "Omit duplicate refs from output."),
		cmdkit.BoolOption("recursive", "r", "Recursively list links of child nodes."),
		cmdkit.IntOption("max-depth", "Only for recursive refs, limits fetch and listing to the given depth").WithDefault(-1),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		ctx := req.Context()
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmdkit.ErrNormal)
			return
		}

		unique, _, err := req.Option("unique").Bool()
		if err != nil {
			res.SetError(err, cmdkit.ErrNormal)
			return
		}

		recursive, _, err := req.Option("recursive").Bool()
		if err != nil {
			res.SetError(err, cmdkit.ErrNormal)
			return
		}

		maxDepth, _, err := req.Option("max-depth").Int()
		if err != nil {
			res.SetError(err, cmdkit.ErrNormal)
			return
		}

		if !recursive {
			maxDepth = 1 // write only direct refs
		}

		format, _, err := req.Option("format").String()
		if err != nil {
			res.SetError(err, cmdkit.ErrNormal)
			return
		}

		edges, _, err := req.Option("edges").Bool()
		if err != nil {
			res.SetError(err, cmdkit.ErrNormal)
			return
		}
		if edges {
			if format != "<dst>" {
				res.SetError(errors.New("using format argument with edges is not allowed"),
					cmdkit.ErrClient)
				return
			}

			format = "<src> -> <dst>"
		}

		objs, err := objectsForPaths(ctx, n, req.Arguments())
		if err != nil {
			res.SetError(err, cmdkit.ErrNormal)
			return
		}

		out := make(chan interface{})
		res.SetOutput((<-chan interface{})(out))

		go func() {
			defer close(out)

			rw := RefWriter{
				out:      out,
				DAG:      n.DAG,
				Ctx:      ctx,
				Unique:   unique,
				PrintFmt: format,
				MaxDepth: maxDepth,
			}

			for _, o := range objs {
				if _, err := rw.WriteRefs(o); err != nil {
					select {
					case out <- &RefWrapper{Err: err.Error()}:
					case <-ctx.Done():
					}
					return
				}
			}
		}()
	},
	Marshalers: refsMarshallerMap,
	Type:       RefWrapper{},
}

var RefsLocalCmd = &cmds.Command{
	Helptext: cmdkit.HelpText{
		Tagline: "List all local references.",
		ShortDescription: `
Displays the hashes of all local objects.
`,
	},

	Run: func(req cmds.Request, res cmds.Response) {
		ctx := req.Context()
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmdkit.ErrNormal)
			return
		}

		// todo: make async
		allKeys, err := n.Blockstore.AllKeysChan(ctx)
		if err != nil {
			res.SetError(err, cmdkit.ErrNormal)
			return
		}

		out := make(chan interface{})
		res.SetOutput((<-chan interface{})(out))

		go func() {
			defer close(out)

			for k := range allKeys {
				select {
				case out <- &RefWrapper{Ref: k.String()}:
				case <-req.Context().Done():
					return
				}
			}
		}()
	},
	Marshalers: refsMarshallerMap,
	Type:       RefWrapper{},
}

var refsMarshallerMap = cmds.MarshalerMap{
	cmds.Text: func(res cmds.Response) (io.Reader, error) {
		v, err := unwrapOutput(res.Output())
		if err != nil {
			return nil, err
		}

		obj, ok := v.(*RefWrapper)
		if !ok {
			return nil, e.TypeErr(obj, v)
		}

		if obj.Err != "" {
			return nil, errors.New(obj.Err)
		}

		return strings.NewReader(obj.Ref + "\n"), nil
	},
}

func objectsForPaths(ctx context.Context, n *core.IpfsNode, paths []string) ([]ipld.Node, error) {
	objects := make([]ipld.Node, len(paths))
	for i, sp := range paths {
		p, err := path.ParsePath(sp)
		if err != nil {
			return nil, err
		}

		o, err := core.Resolve(ctx, n.Namesys, n.Resolver, p)
		if err != nil {
			return nil, err
		}
		objects[i] = o
	}
	return objects, nil
}

type RefWrapper struct {
	Ref string
	Err string
}

type RefWriter struct {
	out chan interface{}
	DAG ipld.DAGService
	Ctx context.Context

	Unique   bool
	MaxDepth int
	PrintFmt string

	seen map[string]int
}

// WriteRefs writes refs of the given object to the underlying writer.
func (rw *RefWriter) WriteRefs(n ipld.Node) (int, error) {
	// refs does not check the root and starts directly at depth 1
	return rw.writeRefsRecursive(n, 1)

}

func (rw *RefWriter) writeRefsRecursive(n ipld.Node, depth int) (int, error) {
	nc := n.Cid()

	var count int
	for i, ng := range ipld.GetDAG(rw.Ctx, rw.DAG, n) {
		lc := n.Links()[i].Cid
		keepGoing, seen := rw.visit(lc, depth)

		// Keep exploring if unvisited
		if !keepGoing {
			continue
		}

		// Write edge if not seen before
		if !seen || seen && !rw.Unique {
			if err := rw.WriteEdge(nc, lc, n.Links()[i].Name); err != nil {
				return count, err
			}
		}

		nd, err := ng.Get(rw.Ctx)
		if err != nil {
			return count, err
		}

		c, err := rw.writeRefsRecursive(nd, depth+1)
		count += c
		if err != nil {
			return count, err
		}
	}
	return count, nil
}

// visit returns false if we should not re-explore the tree under a Cid, or
// true otherwise. The second return argument indicates whether the Cid was seen
// before.
func (rw *RefWriter) visit(c *cid.Cid, depth int) (bool, bool) {
	if rw.seen == nil {
		rw.seen = make(map[string]int)
	}

	key := string(c.Bytes())
	oldDepth, ok := rw.seen[key]

	// we should never explore over max-depth
	if rw.MaxDepth >= 0 && depth > rw.MaxDepth {
		return false, ok
	}

	// normally we would shortcut an already visited branch,
	// but we should re-explore when non-unique
	if ok && rw.MaxDepth < 0 {
		return !rw.Unique, ok
	}

	// we always re-explore when the cid is new or
	// we need to go deeper than before
	if !ok || oldDepth > depth {
		rw.seen[key] = depth
		return true, ok
	}

	// The final case by elimination is:
	// - ok is true (we visited before)
	// - oldDepth <= depth (we visited higher in the tree)
	// In this case we only keep going if don't want Unique cids.
	return !rw.Unique, ok
}

// Write one edge
func (rw *RefWriter) WriteEdge(from, to *cid.Cid, linkname string) error {
	if rw.Ctx != nil {
		select {
		case <-rw.Ctx.Done(): // just in case.
			return rw.Ctx.Err()
		default:
		}
	}

	var s string
	switch {
	case rw.PrintFmt != "":
		s = rw.PrintFmt
		s = strings.Replace(s, "<src>", from.String(), -1)
		s = strings.Replace(s, "<dst>", to.String(), -1)
		s = strings.Replace(s, "<linkname>", linkname, -1)
	default:
		s += to.String()
	}

	rw.out <- &RefWrapper{Ref: s}
	return nil
}
