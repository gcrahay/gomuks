package matrix

import (
	"fmt"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	"maunium.net/go/gomuks/matrix/muksevt"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
	"os"
)

type EventIndex struct {
	index bleve.Index
}

type Event struct {
	RoomID  id.RoomID `json:"room"`
	Content string    `json:"content"`
}

func (e Event) Type() string {
	return "event"
}

func NewEventIndex(dbPath string) (*EventIndex, error) {

	var index bleve.Index
	var err error
	_, err = os.Stat(dbPath)
	if os.IsNotExist(err) {
		roomMapping := bleve.NewTextFieldMapping()
		roomMapping.Analyzer = keyword.Name
		eventMapping := bleve.NewDocumentMapping()
		eventMapping.AddFieldMappingsAt("room", roomMapping)
		mapping := bleve.NewIndexMapping()
		mapping.AddDocumentMapping("event", eventMapping)
		if index, err = bleve.New(dbPath, mapping); err != nil {
			return nil, err
		}
	} else if err == nil {
		if index, err = bleve.Open(dbPath); err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}

	return &EventIndex{index: index}, nil
}

func (index *EventIndex) Put(roomID id.RoomID, evt muksevt.Event) error {
	if evt.Type != event.EventMessage {
		return fmt.Errorf("not a message event")
	}
	msg, ok := evt.Content.Parsed.(*event.MessageEventContent)
	if !ok {
		return fmt.Errorf("unparseable content")
	}
	toIndex := Event{
		RoomID:  roomID,
		Content: msg.Body,
	}
	return index.index.Index(string(evt.ID), toIndex)
}

func (index *EventIndex) SearchRoom(roomID id.RoomID, filter string) ([]id.EventID, error) {
	query := bleve.NewMatchQuery(filter)
	search := bleve.NewSearchRequest(query)
	results, err := index.index.Search(search)
	if err != nil {
		return nil, err
	}
	ids := make([]id.EventID, 0, results.Size())
	for _, result := range results.Hits {
		ids = append(ids, id.EventID(result.ID))
	}
	return ids, nil
}
